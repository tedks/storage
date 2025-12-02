# Vendored dependencies

Use this directory for third-party code that needs BUILD metadata or other customization that cannot be handled purely by the language package managers.

Workflow (per repository):
1. Fork the upstream repository on GitHub.
2. Create a branch in your fork named `<project>_master` (e.g. `storage_master`).
3. Add the necessary BUILD/metadata changes on that branch.
4. Add the fork as a git submodule under `third_party/<repo>` and track the `<project>_master` branch.
5. Use `third_party/update.sh` to periodically pull from upstream into `<project>_master` and refresh the submodule.

Each vendored repo should contain a `BUILD` file that exposes its targets with `//visibility:public`.
