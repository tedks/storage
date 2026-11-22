/* Experimental Linux interposer for Nautilus's native directory monitor. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <linux/magic.h>
#include <linux/openat2.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <gio/gio.h>
#include <json-glib/json-glib.h>
#include <string.h>
#include <unistd.h>

#define MAX_PATH_BYTES 768
#define MAX_NAME_BYTES 255
#define MAX_ADDITIONS 128
#define MAX_RESPONSE_BYTES 67584
#define MAX_MONITORS 16
#define POLL_MS 500

typedef struct {
    gatomicrefcount refs;
    GWeakRef monitor;
    GFile *directory;
    char *mount;
    char *relative;
    char *mount_hex;
    char *directory_hex;
    char *endpoint;
    gint64 attached;
    char *generation;
    gint64 through;
    guint64 mount_id;
    gboolean pending;
    gboolean stopped;
} Subscription;

typedef struct {
    char *generation;
    gint64 through;
    guint64 mount_id;
    GPtrArray *names;
} Additions;

static gint active_monitors;

static void subscription_unref(gpointer data)
{
    Subscription *sub = data;
    if (!g_atomic_ref_count_dec(&sub->refs))
        return;
    g_weak_ref_clear(&sub->monitor);
    g_object_unref(sub->directory);
    g_free(sub->mount);
    g_free(sub->relative);
    g_free(sub->mount_hex);
    g_free(sub->directory_hex);
    g_free(sub->endpoint);
    g_free(sub->generation);
    g_free(sub);
    g_atomic_int_add(&active_monitors, -1);
}

static void additions_free(gpointer data)
{
    Additions *additions = data;
    if (additions == NULL)
        return;
    g_free(additions->generation);
    g_ptr_array_unref(additions->names);
    g_free(additions);
}

static char *hex_encode(const char *raw)
{
    static const char alphabet[] = "0123456789abcdef";
    gsize length = strlen(raw);
    char *hex = g_malloc(2 * length + 1);
    for (gsize pos = 0; pos < length; pos++) {
        guint8 byte = (guint8)raw[pos];
        hex[2 * pos] = alphabet[byte >> 4];
        hex[2 * pos + 1] = alphabet[byte & 15];
    }
    hex[2 * length] = '\0';
    return hex;
}

static char *decode_name(const char *hex)
{
    if (hex == NULL)
        return NULL;
    gsize length = strlen(hex);
    if (length == 0 || length > 2 * MAX_NAME_BYTES || length % 2 != 0)
        return NULL;
    g_autofree char *name = g_malloc(length / 2 + 1);
    for (gsize pos = 0; pos < length; pos += 2) {
        int high = g_ascii_xdigit_value(hex[pos]);
        int low = g_ascii_xdigit_value(hex[pos + 1]);
        if (high < 0 || low < 0)
            return NULL;
        char byte = (char)(16 * high + low);
        if (byte == '\0' || byte == '/')
            return NULL;
        name[pos / 2] = byte;
    }
    name[length / 2] = '\0';
    if (!strcmp(name, ".") || !strcmp(name, ".."))
        return NULL;
    return g_steal_pointer(&name);
}

static const char *json_string(JsonNode *node)
{
    return node != NULL && json_node_get_value_type(node) == G_TYPE_STRING
           ? json_node_get_string(node) : NULL;
}

static Additions *parse_additions(const char *frame, gsize length, const Subscription *sub)
{
    g_autoptr(JsonParser) parser = json_parser_new();
    if (!json_parser_load_from_data(parser, frame, length, NULL))
        return NULL;
    JsonNode *root = json_parser_get_root(parser);
    if (root == NULL || !JSON_NODE_HOLDS_OBJECT(root))
        return NULL;
    JsonObject *object = json_node_get_object(root);
    const char *status = json_string(json_object_get_member(object, "status"));
    const char *generation = json_string(json_object_get_member(object, "generation"));
    JsonNode *cursor = json_object_get_member(object, "through");
    JsonNode *items = json_object_get_member(object, "added");
    if (g_strcmp0(status, "ok") != 0 || generation == NULL ||
        strlen(generation) != 32 || strspn(generation, "0123456789abcdef") != 32 ||
        (sub->generation != NULL && strcmp(generation, sub->generation) != 0) ||
        cursor == NULL || json_node_get_value_type(cursor) != G_TYPE_INT64 ||
        items == NULL || !JSON_NODE_HOLDS_ARRAY(items))
        return NULL;

    gint64 through = json_node_get_int(cursor);
    JsonArray *array = json_node_get_array(items);
    guint count = json_array_get_length(array);
    if (through < sub->through || count > MAX_ADDITIONS ||
        count > (guint64)(through - sub->through))
        return NULL;

    g_autoptr(GPtrArray) names = g_ptr_array_new_with_free_func(g_free);
    for (guint index = 0; index < count; index++) {
        char *name = decode_name(json_string(json_array_get_element(array, index)));
        if (name == NULL)
            return NULL;
        g_ptr_array_add(names, name);
    }
    Additions *additions = g_new0(Additions, 1);
    additions->generation = g_strdup(generation);
    additions->through = through;
    additions->names = g_steal_pointer(&names);
    return additions;
}

static gboolean socket_ready(GSocket *socket, GIOCondition mode, gint64 deadline)
{
    gint64 remaining = deadline - g_get_monotonic_time();
    return remaining > 0 &&
           g_socket_condition_timed_wait(socket, mode, remaining, NULL, NULL);
}

/* Resolve in the worker. NO_XDEV relative to the selected FUSE root
 * rejects nested mounts, and NO_SYMLINKS keeps the virtual key literal. */
static guint64 mount_id(const Subscription *sub)
{
    struct open_how how = {
        .flags = O_PATH | O_DIRECTORY | O_CLOEXEC,
        .resolve = RESOLVE_NO_SYMLINKS,
    };
    int root = syscall(SYS_openat2, AT_FDCWD, sub->mount, &how, sizeof(how));
    if (root < 0)
        return 0;
    struct statfs fs;
    struct statx info;
    guint64 id = 0;
    how.resolve |= RESOLVE_NO_XDEV | RESOLVE_BENEATH;
    int directory = syscall(SYS_openat2, root, sub->relative, &how, sizeof(how));
    if (directory >= 0 && fstatfs(root, &fs) == 0 && fs.f_type == FUSE_SUPER_MAGIC &&
        statx(root, "", AT_EMPTY_PATH | AT_STATX_DONT_SYNC, STATX_MNT_ID, &info) == 0 &&
        (info.stx_mask & STATX_MNT_ID))
        id = info.stx_mnt_id;
    if (directory >= 0)
        close(directory);
    close(root);
    return id;
}

static Additions *request_additions(const Subscription *sub)
{
    g_autoptr(GSocketClient) client = g_socket_client_new();
    g_socket_client_set_enable_proxy(client, FALSE);
    g_socket_client_set_timeout(client, 1);
    g_autoptr(GSocketAddress) address = g_unix_socket_address_new(sub->endpoint);
    g_autoptr(GSocketConnection) connection =
        g_socket_client_connect(client, G_SOCKET_CONNECTABLE(address), NULL, NULL);
    if (connection == NULL)
        return NULL;
    GSocket *socket = g_socket_connection_get_socket(connection);
    g_autoptr(GCredentials) peer = g_socket_get_credentials(socket, NULL);
    if (peer == NULL || g_credentials_get_unix_user(peer, NULL) != geteuid())
        return NULL;

    g_autofree char *cursor = sub->generation == NULL
        ? g_strdup_printf("\"since_us\":%" G_GINT64_FORMAT, sub->attached)
        : g_strdup_printf("\"generation\":\"%s\",\"after\":%" G_GINT64_FORMAT,
                          sub->generation, sub->through);
    g_autofree char *request =
        g_strdup_printf("{\"command\":\"desktop.additions.v1\",\"mount_hex\":\"%s\","
                        "\"directory_hex\":\"%s\",%s}\n",
                        sub->mount_hex, sub->directory_hex, cursor);
    g_socket_set_blocking(socket, FALSE);
    gint64 deadline = g_get_monotonic_time() + G_TIME_SPAN_SECOND;
    gsize sent = 0;
    gsize length = strlen(request);
    while (sent < length && socket_ready(socket, G_IO_OUT, deadline)) {
        gssize written = g_socket_send(socket, request + sent, length - sent, NULL, NULL);
        if (written <= 0)
            return NULL;
        sent += written;
    }
    if (sent != length)
        return NULL;

    g_autofree char *frame = g_malloc(MAX_RESPONSE_BYTES);
    gsize received = 0;
    while (received < MAX_RESPONSE_BYTES && socket_ready(socket, G_IO_IN, deadline)) {
        gssize count = g_socket_receive(socket, frame + received,
                                        MAX_RESPONSE_BYTES - received, NULL, NULL);
        if (count <= 0)
            return NULL;
        char *lf = memchr(frame + received, '\n', count);
        if (lf != NULL)
            return parse_additions(frame, lf - frame, sub);
        received += count;
    }
    return NULL;
}

static void worker(GTask *task, gpointer source, gpointer data, GCancellable *cancellable)
{
    (void)source;
    (void)cancellable;
    const Subscription *sub = data;
    guint64 id = mount_id(sub);
    Additions *additions = NULL;
    if (id != 0 && (sub->mount_id == 0 || id == sub->mount_id)) {
        additions = request_additions(sub);
        if (additions != NULL) {
            if (additions->names->len != 0 && id != mount_id(sub))
                g_clear_pointer(&additions, additions_free);
            else
                additions->mount_id = id;
        }
    }
    g_task_return_pointer(task, additions, additions_free);
}

static void delivered(GObject *source, GAsyncResult *result, gpointer data)
{
    (void)source;
    Subscription *sub = data;
    Additions *additions = g_task_propagate_pointer(G_TASK(result), NULL);
    g_autoptr(GFileMonitor) monitor = g_weak_ref_get(&sub->monitor);
    if (monitor != NULL && !g_file_monitor_is_cancelled(monitor)) {
        if (additions == NULL) {
            sub->stopped = TRUE;
            g_warning("Storage desktop additions stopped: reconnect or delivery gap; reopen the directory");
        } else {
            g_free(sub->generation);
            sub->generation = g_strdup(additions->generation);
            sub->through = additions->through;
            sub->mount_id = additions->mount_id;
            for (guint index = 0; index < additions->names->len; index++) {
                if (g_file_monitor_is_cancelled(monitor))
                    break;
                const char *name = g_ptr_array_index(additions->names, index);
                g_autoptr(GFile) child = g_file_get_child(sub->directory, name);
                g_file_monitor_emit_event(monitor, child, NULL, G_FILE_MONITOR_EVENT_CREATED);
            }
        }
    }
    additions_free(additions);
    sub->pending = FALSE;
}

static gboolean poll_subscription(gpointer data)
{
    Subscription *sub = data;
    g_autoptr(GFileMonitor) monitor = g_weak_ref_get(&sub->monitor);
    if (sub->stopped || monitor == NULL || g_file_monitor_is_cancelled(monitor))
        return G_SOURCE_REMOVE;
    if (!sub->pending) {
        sub->pending = TRUE;
        GMainContext *context = g_source_get_context(g_main_current_source());
        g_main_context_push_thread_default(context);
        g_autoptr(GTask) task = g_task_new(NULL, NULL, delivered, sub);
        g_main_context_pop_thread_default(context);
        g_atomic_ref_count_inc(&sub->refs);
        g_task_set_task_data(task, sub, subscription_unref);
        g_task_run_in_thread(task, worker);
    }
    return G_SOURCE_CONTINUE;
}

static gboolean is_nautilus(void)
{
    static gsize checked = 0;
    if (g_once_init_enter(&checked)) {
        g_autofree char *exe = g_file_read_link("/proc/self/exe", NULL);
        g_autofree char *name = exe != NULL ? g_path_get_basename(exe) : NULL;
        g_once_init_leave(&checked, g_strcmp0(name, "nautilus") == 0 ? 1 : 2);
    }
    return checked == 1;
}

GFileMonitor *g_file_monitor_directory(GFile *file, GFileMonitorFlags flags,
                                       GCancellable *cancellable, GError **error)
{
    typedef GFileMonitor *(*MonitorDirectory)(GFile *, GFileMonitorFlags,
                                               GCancellable *, GError **);
    static gsize symbol = 0;
    if (g_once_init_enter(&symbol)) {
        MonitorDirectory delegate = (MonitorDirectory)dlsym(RTLD_NEXT, "g_file_monitor_directory");
        if (delegate == NULL)
            g_error("Storage desktop additions could not find the native GIO monitor");
        g_once_init_leave(&symbol, (gsize)delegate);
    }

    gint64 attached = g_get_monotonic_time();
    GFileMonitor *monitor = ((MonitorDirectory)symbol)(file, flags, cancellable, error);
    const char *mount = g_getenv("STORAGE_NAUTILUS_MOUNT");
    const char *socket = g_getenv("STORAGE_NAUTILUS_SOCKET");
    if (monitor == NULL || mount == NULL || socket == NULL || !is_nautilus() ||
        !g_file_is_native(file))
        return monitor;

    g_autofree char *path = g_file_get_path(file);
    gsize base = strlen(mount);
    if (path == NULL || mount[0] != '/' || base < 2 ||
        !g_str_has_prefix(path, mount) || (path[base] != '/' && path[base] != '\0'))
        return monitor;
    const char *relative = path[base] == '\0' ? "/" : path + base;
    if (base > MAX_PATH_BYTES || strlen(relative) > MAX_PATH_BYTES ||
        socket[0] != '/' || strlen(socket) > 107) {
        g_warning("Storage desktop additions stopped: mount, directory or endpoint exceeds supported limits");
        return monitor;
    }
    if (g_atomic_int_add(&active_monitors, 1) >= MAX_MONITORS) {
        g_atomic_int_add(&active_monitors, -1);
        g_warning("Storage desktop additions stopped: too many active monitors");
        return monitor;
    }

    Subscription *sub = g_new0(Subscription, 1);
    g_atomic_ref_count_init(&sub->refs);
    g_weak_ref_init(&sub->monitor, monitor);
    sub->directory = g_object_ref(file);
    sub->mount = g_strdup(mount);
    sub->relative = g_strdup(path[base] == '\0' ? "." : path + base + 1);
    sub->mount_hex = hex_encode(mount);
    sub->directory_hex = hex_encode(relative);
    sub->endpoint = g_strdup(socket);
    sub->attached = attached;
    GSource *poll = g_timeout_source_new(POLL_MS);
    g_source_set_callback(poll, poll_subscription, sub, subscription_unref);
    g_autoptr(GMainContext) context = g_main_context_ref_thread_default();
    g_source_attach(poll, context);
    g_source_unref(poll);
    return monitor;
}
