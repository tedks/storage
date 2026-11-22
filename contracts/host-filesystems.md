# Contract for SSH hosts serving multiple Storage members

**Consumers.** SSH/SFTP filesystem adapters, rsync adapters, watcher, concurrency planners, and mount lifecycle.

- Group transport use within one mount by effective endpoint, port, login, authentication, route and host-verification policy. Keep differing or unverifiable policies isolated. Resolving two hostnames to one IP is insufficient evidence of compatibility. A root path, display name or member priority does not create a new host identity.
- Open lazily with one connection attempt in progress for the identity. Lease independent channels for simultaneous operations. Apply transport limits, interactive reserve and server channel-limit feedback at host scope. A saturated bulk queue cannot consume the interactive reserve. A child awaiting a channel must honor its operation's cancellation and timeout contract.
- Member identity, root translation, root health, cache keys, repair records and replication destinations stay independent. A valid session plus an inaccessible root is a filesystem failure. A failed session invalidates its generation. A stale lease returned later cannot close a replacement generation.
- The registry coordinates reconnect attempts and reference lifetimes. Closing or failing one filesystem leaves healthy siblings usable. Last release or mount shutdown reclaims owned resources. Do not replay an uncertain mutation to establish connectivity. Pre-fork probes finish before daemonization.
- Count one steady-state SSH connection per compatible identity for the traffic actually routed through the registry. Separately identify transient reconnects, startup probes and any subprocesses still using another provider. Include watcher and rsync sessions in the server-side count before claiming the mixed case.

Verify with concurrent real-server reads of different bytes at identical relative names, a missing root alongside a healthy root, separate credentials, close/reconnect and foreground progress at the host's capacity limit. Use synthetic fixtures for counters and diagnostics.
