ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}

RUN apt-get -qq update && apt-get -qq -y --no-install-recommends install \
    ca-certificates \
    curl \
    docker.io \
    libsasl2-dev \
    netcat \
    openjdk-11-jdk \
    sudo \
    zip &&\
    rm -rf /var/lib/apt/lists/*

ARG UID={${UID}:-1001}
ENV USER_ID=${LOCAL_USER_ID:-${UID}}

RUN echo '{"storage-driver": "vfs"}' > /etc/docker/daemon.json

# setup user
RUN useradd --shell /bin/bash -u $USER_ID --gid 0 --non-unique --comment "" --create-home user
RUN usermod -a -G sudo user
RUN usermod -a -G docker user
RUN echo "user ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/user


# connection to ha.pool.sks-keyservers.net fails sometimes, so let's retry with couple different servers
RUN for server in $(shuf -e ha.pool.sks-keyservers.net \
                            hkp://p80.pool.sks-keyservers.net:80 \
                            keyserver.ubuntu.com \
                            hkp://keyserver.ubuntu.com:80 \
                            pgp.mit.edu) ; do gpg --no-tty --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && s=0 && break || s=$?; done; (exit $s)

RUN curl -o /usr/local/bin/gosu -sSL "https://github.com/tianon/gosu/releases/download/1.14/gosu-$(dpkg --print-architecture)" \
    && curl -o /usr/local/bin/gosu.asc -sSL "https://github.com/tianon/gosu/releases/download/1.14/gosu-$(dpkg --print-architecture).asc" \
    && gpg --verify /usr/local/bin/gosu.asc \
    && rm /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu

COPY tests/entrypoint.sh /usr/local/bin/entrypoint.sh

COPY requirements.txt /

# if we're in a pypy image, link pypy/pypy3 to /usr/local/bin/python
RUN if command -v pypy3; then ln -s $(command -v pypy3) /usr/local/bin/python; elif command -v pypy; then ln -s $(command -v pypy) /usr/local/bin/python; fi

RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
