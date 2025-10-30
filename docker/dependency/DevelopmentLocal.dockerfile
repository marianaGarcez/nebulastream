# This image is build locally on a developers machine. It installs the current user into the container instead of
# relying on the root user. Ubuntu 24 by default installs the ubuntu user which is replaced.
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

USER root
ARG UID=1000
ARG GID=1000
ARG USERNAME=ubuntu
ARG ROOTLESS=false

# Install MEOS build dependencies and build MobilityDB MEOS into /usr/local
RUN apt-get update -y && apt-get install -y \
    git cmake build-essential pkg-config \
    libgeos-dev libproj-dev libgsl-dev libjson-c-dev \
 && rm -rf /var/lib/apt/lists/* \
 && if [ ! -f /usr/local/include/meos.h ]; then \
      git clone https://github.com/MobilityDB/MobilityDB.git /tmp/MobilityDB && \
      cmake -S /tmp/MobilityDB -B /tmp/MobilityDB/build -DMEOS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local && \
      cmake --build /tmp/MobilityDB/build -j"$(nproc || echo 4)" && \
      cmake --install /tmp/MobilityDB/build && \
      ldconfig && \
      rm -rf /tmp/MobilityDB; \
    fi

RUN (${ROOTLESS} || (echo "uid: ${UID} gid ${GID} username ${USERNAME}" && \
    (delgroup ubuntu || true) && \
    (deluser ubuntu || true) && \
    addgroup --gid ${GID} ${USERNAME} && \
    adduser --uid ${UID} --gid ${GID} ${USERNAME})) && \
    chown -R ${UID}:${GID} ${NES_PREBUILT_VCPKG_ROOT}

USER ${USERNAME}
