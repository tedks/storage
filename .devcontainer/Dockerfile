# Minimal devcontainer image with Nix and flakes enabled.
FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    USER=dev \
    UID=1000 \
    GID=1000 \
    NIX_BUILD_CORES=4 \
    NIX_CONFIG="experimental-features = nix-command flakes"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates git sudo xz-utils && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user matching typical host uid/gid for volume permission parity.
RUN groupadd -g ${GID} ${USER} && \
    useradd -m -u ${UID} -g ${GID} -s /bin/bash ${USER} && \
    echo "${USER} ALL=(ALL) NOPASSWD:ALL" >/etc/sudoers.d/90-${USER}

# Install Nix (single-user mode inside container).
USER ${USER}
ENV USER=${USER} HOME=/home/${USER}
RUN sh <(curl -L https://nixos.org/nix/install) --no-daemon && \
    . "$HOME/.nix-profile/etc/profile.d/nix.sh" && \
    nix --version

SHELL ["/bin/bash", "-lc"]

# Pre-warm flake dependencies for faster startup.
WORKDIR /workspace
COPY --chown=${USER}:${USER} . /workspace
RUN . "$HOME/.nix-profile/etc/profile.d/nix.sh" && \
    nix develop . --command true

# Enter flake shell on attach.
CMD ["/bin/bash", "-lc", ". \"$HOME/.nix-profile/etc/profile.d/nix.sh\" && nix develop ."]
