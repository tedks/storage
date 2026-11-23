/* Drives the native bridge in a non-default main context, without GTK. */
#include <gio/gio.h>

typedef struct {
    GMainContext *context;
    GFile *expected;
    unsigned arrivals;
} Observer;

static void changed(GFileMonitor *monitor, GFile *file, GFile *other,
                    GFileMonitorEvent event, gpointer data)
{
    (void)monitor;
    (void)other;
    Observer *observer = data;
    g_assert_true(g_main_context_is_owner(observer->context));
    g_assert_cmpint(event, ==, G_FILE_MONITOR_EVENT_CREATED);
    g_assert_true(g_file_equal(observer->expected, file));
    observer->arrivals++;
}

static gboolean finish(gpointer data)
{
    g_main_loop_quit(data);
    return G_SOURCE_REMOVE;
}

int main(int argc, char **argv)
{
    g_assert_cmpint(argc, ==, 3);
    unsigned expected_arrivals = (unsigned)g_ascii_strtoull(argv[2], NULL, 10);
    g_autofree char *mount = g_build_filename(argv[1], "mount", NULL);
    g_autofree char *release = g_build_filename(argv[1], "release", NULL);
    g_autoptr(GFile) root = g_file_new_for_path(mount);
    g_autoptr(GFile) late = g_file_get_child(root, "late-visible.txt");
    g_autoptr(GMainContext) context = g_main_context_new();
    g_main_context_push_thread_default(context);
    g_autoptr(GError) error = NULL;
    g_autoptr(GFileMonitor) active = g_file_monitor_directory(root, 0, NULL, &error);
    g_assert_no_error(error);
    g_assert_nonnull(active);
    g_autoptr(GFileMonitor) cancelled = g_file_monitor_directory(root, 0, NULL, &error);
    g_assert_no_error(error);
    g_assert_nonnull(cancelled);
    g_main_context_pop_thread_default(context);
    Observer listening = { .context = context, .expected = late };
    Observer detached = { .context = context, .expected = late };
    g_signal_connect(active, "changed", G_CALLBACK(changed), &listening);
    g_signal_connect(cancelled, "changed", G_CALLBACK(changed), &detached);
    g_assert_true(g_file_monitor_cancel(cancelled));

    g_autoptr(GFileEnumerator) children = g_file_enumerate_children(
        root, G_FILE_ATTRIBUTE_STANDARD_NAME, 0, NULL, &error);
    g_assert_no_error(error);
    g_assert_nonnull(children);
    g_autoptr(GFileInfo) first = g_file_enumerator_next_file(children, NULL, &error);
    g_assert_no_error(error);
    g_assert_nonnull(first);
    g_assert_cmpstr(g_file_info_get_name(first), ==, "first-visible.txt");
    g_autoptr(GFileInfo) next = g_file_enumerator_next_file(children, NULL, &error);
    g_assert_no_error(error);
    g_assert_null(next);

    gboolean released = g_file_set_contents(release, "", 0, &error);
    g_assert_no_error(error);
    g_assert_true(released);
    g_autoptr(GMainLoop) loop = g_main_loop_new(context, FALSE);
    g_autoptr(GSource) timeout = g_timeout_source_new_seconds(2);
    g_source_set_callback(timeout, finish, loop, NULL);
    g_source_attach(timeout, context);
    g_main_loop_run(loop);
    g_assert_cmpuint(listening.arrivals, ==, expected_arrivals);
    g_assert_cmpuint(detached.arrivals, ==, 0);
    g_file_monitor_cancel(active);
    return 0;
}
