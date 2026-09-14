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
      in
      {
        devShells.default = pkgs.mkShell ({
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
            echo "Hint: run 'direnv allow' after adding the hook to your shell to auto-enter this flake."
          '';
        } // pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # fusepy loads libfuse 2 (libfuse.so.2), which many hosts no longer
          # install. Point it at the nixpkgs build instead of the host's.
          # Only the library output: pkgs.fuse in `packages` would put its
          # non-setuid fusermount on PATH ahead of the host's setuid one.
          # This libfuse still finds fusermount at /run/wrappers/bin (NixOS)
          # or on PATH. It is loaded into a host-glibc Python, so it needs a
          # host glibc new enough for this nixpkgs build.
          FUSE_LIBRARY_PATH = "${pkgs.fuse.out}/lib/libfuse.so.2";
        });
      });
}
