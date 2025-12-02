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
            echo "Hint: run 'direnv allow' after adding the hook to your shell to auto-enter this flake."
          '';
        };
      });
}
