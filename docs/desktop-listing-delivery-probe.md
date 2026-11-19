# Desktop delivery for progressive listings

The stock local-file monitor on the tested Linux stack does not deliver a
late FUSE listing addition to an already-open Nautilus directory. A scratch
client-side monitor bridge did deliver the addition on the existing `file://`
path. Production desktop delivery remains unsupported pending agreement on
an opt-in client component. Progressive listing publication can proceed
independently and is observable on the next enumeration.

## Scratch capability test

The test used Linux `7.0.0-42-generic`, libfuse `2.9.9`, GLib/GIO `2.80.0`,
and installed Nautilus `46.4`. It ran against a local low-level FUSE fixture
with synthetic directory names. Nautilus ran under a private X display and
D-Bus session. Its accessibility tree showed which items the open view
actually displayed.

A producer committed each change to the fixture's namespace before invoking
FUSE invalidation. The monitor was already open. The entry-add case primed a
negative lookup first, and the test checked that each operation changed the
lookup or attribute result as intended. The observed monitor backend was
`GInotifyFileMonitor`.

| Operation | Lookup or metadata after the operation | GIO event |
| --- | --- | --- |
| Commit a late name, then `fuse_lowlevel_notify_inval_entry` | Previously missing name resolves | None |
| Commit a new mode, then `fuse_lowlevel_notify_inval_inode` | Updated mode resolves | None |
| Commit a removal, then `fuse_lowlevel_notify_delete` | Previously present name no longer resolves | None |
| Control case, `mkdir` through the mounted path | New directory resolves | `CREATED` and `CHANGES_DONE_HINT` |

All three notifications returned success. Linux's FUSE handler invalidates
kernel metadata and dentries, and its delete path can drop the named dentry.
Those paths have no matching file-monitor event in the tested cases.
[Linux v7.0 FUSE notification dispatch](https://github.com/torvalds/linux/blob/v7.0/fs/fuse/dev.c#L1676-L1757),
[Linux v7.0 FUSE entry invalidation](https://github.com/torvalds/linux/blob/v7.0/fs/fuse/dir.c#L1581-L1653).

The stock Nautilus window continued to show its initial item after late
publication. The late item stayed absent and there was no follow-up root
enumeration. A genuine `mkdir` through the mount made the control item
appear in that same window while the late item remained absent.

## Scoped client-side feasibility

The scratch bridge was loaded only into a fresh Nautilus process. It used
`LD_PRELOAD` to intercept `g_file_monitor_directory`, delegated to the
installed implementation, and selected the returned monitor for one exact
native directory. After publishing the late fixture name, the test sent a
one-shot signal over a private FIFO. On Nautilus's main context the bridge
emitted `G_FILE_MONITOR_EVENT_CREATED` for that child on the matching
monitor. The accessibility tree gained the late item without another root
enumeration. The bridge issued no window action, navigation, or filesystem
mutation. The same view without the bridge served as the failure control.
[Nautilus 46.4 monitor consumer](https://github.com/GNOME/nautilus/blob/46.4/src/nautilus-monitor.c),
[GIO monitor emission API and context requirement](https://docs.gtk.org/gio/method.FileMonitor.emit_event.html).

An opt-in client would need to start with the component loaded. An existing
Nautilus process would need to restart with that configuration. Each
subscription could attach to a native directory monitor for an explicitly
selected mount and expire when the monitor is cancelled or released, or when
its mount generation ends. Delivery would target the monitor that created
the subscription, on that monitor's main context. Tab navigation would
therefore have no bearing on the event's target. A production implementation
would need an authenticated and bounded post-commit transport, cancellation,
revision and mount-generation checks, and recovery after gaps. The one-shot
probe proves the client event path for an addition. It did not exercise those
transport and lifecycle requirements or the other change types.

A stock Nautilus `reload` window action targets the active tab at execution.
Its window-to-locations property lists tabs without identifying the active
tab, and `reload` takes no expected-URI argument. A separate process cannot
use the property and action to atomically reload a specific directory during
navigation. The client-side proof uses the directory monitor directly.
[Nautilus 46.4 window-to-locations property](https://github.com/GNOME/nautilus/blob/46.4/src/nautilus-application.c#L1243-L1308),
[Nautilus 46.4 reload action](https://github.com/GNOME/nautilus/blob/46.4/src/nautilus-window.c#L237-L245).

## Other tabs during a pending listing

A separate scratch trial used one stock Nautilus 46.4 process, one window,
and one threaded libfuse 2.9.9 mount on the same Linux and GIO versions.
Each case held one `readdir` at a test gate while Nautilus opened a fresh
second tab. The fixture used a shared admission lock for the two directories
representing one member. Another directory on the mount used independent
admission. The test matched both tab locations to the same window and
process, and checked the displayed rows through AT-SPI.

| Second tab | Observation while the first `readdir` was held |
| --- | --- |
| An unrelated local `file://` directory | Its row appeared before release. |
| A directory sharing the held listing's admission lock | Its FUSE request arrived. Its row stayed absent throughout a one-second observation and appeared after release. |
| An independently admitted directory on the same threaded FUSE mount | Its FUSE reply and visible row arrived before release. |

The trial isolates head-of-line blocking at a shared admission queue as one
way a listing can stall another tab. It covers one held request and one
second tab per case, with synthetic admission and no Storage scheduler or
remote backend in the fixture.
