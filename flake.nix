{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python311;

        # fusepy loads libfuse 2 (libfuse.so.2), which many hosts no longer
        # install. Precedence: a non-empty FUSE_LIBRARY_PATH already in the
        # environment is kept; else a host libfuse 2 in the linker cache is
        # left to fusepy's find_library, because it is built for the host
        # glibc that Bazel's Python uses and nixpkgs' build can need a newer
        # one; else fusepy is pointed at nixpkgs' build. This runs in
        # shellHook (nix develop, nix-direnv), so tools that import only the
        # derivation's variables, e.g. `nix print-dev-env --json`, do not
        # see it.
        # Only the library output: pkgs.fuse2 in `packages` would put its
        # non-setuid fusermount on PATH ahead of the host's setuid one. This
        # libfuse runs /run/wrappers/bin/fusermount (NixOS), then the first
        # fusermount on PATH.
        # Only a host library for this system's ABI counts (an i386 one on
        # x86_64 is not loadable), matched by prefix on the ldconfig -p tag,
        # e.g. "libfuse.so.2 (libc6,x86-64) => ...", as CPython's
        # find_library matches it on x86_64.
        ldconfigAbi = {
          x86_64-linux = "libc6,x86-64";
          aarch64-linux = "libc6,AArch64";
        }.${system} or "libc6";
        libfuse2Hook = pkgs.lib.optionalString pkgs.stdenv.hostPlatform.isLinux ''
          case "$(/sbin/ldconfig -p 2>/dev/null)" in
            *"libfuse.so.2 (${ldconfigAbi}"*) ;;
            *) export FUSE_LIBRARY_PATH="''${FUSE_LIBRARY_PATH:-${pkgs.fuse2.out}/lib/libfuse.so.2}" ;;
          esac
          echo "  libfuse2:  ''${FUSE_LIBRARY_PATH:-host, via find_library}"
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.bazel_7
            pkgs.jdk11
            pkgs.git
            pkgs.direnv
            pkgs.nix-direnv
            python
            python.pkgs.pip
            python.pkgs.virtualenv
          ];

          shellHook = ''
            echo "Entering storage dev shell"
            echo "  bazel:     $(bazel --version)"
            echo "  java:      $(java -version 2>&1 | head -n 1)"
            echo "  python:    $(python3 --version)"
            echo "  pip:       $(pip --version)"
            echo "  virtualenv $(virtualenv --version)"
            echo "  direnv:    $(direnv version 2>/dev/null || true)"
            ${libfuse2Hook}
            echo "Hint: run 'direnv allow' after adding the hook to your shell to auto-enter this flake."
          '';
        };
      });
}
