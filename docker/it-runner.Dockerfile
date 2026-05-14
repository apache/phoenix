# Dockerfile for running Phoenix integration tests on Linux.
# Built and used by /run-it-tests-local.sh — not for production.

FROM eclipse-temurin:17-jdk-jammy

ARG MAVEN_VERSION=3.9.9
ARG MAVEN_SHA=23B11248DCDB9C4DD7C2D69BE2F09CFA01CE5A41819AB31FE893E6FB6CDB52FD9F4F4A6BE51DC0DFA1A20DF9B6A39EC1107B9DD4A3BCEC6B68CFDFEE05A60BC6
ARG MAVEN_TARBALL=apache-maven-${MAVEN_VERSION}-bin.tar.gz
ARG MAVEN_BASE_URL=https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl ca-certificates git lsof procps tini \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "${MAVEN_BASE_URL}/${MAVEN_TARBALL}" -o /tmp/maven.tar.gz \
 && tar -xzf /tmp/maven.tar.gz -C /opt \
 && ln -s /opt/apache-maven-${MAVEN_VERSION} /opt/maven \
 && rm /tmp/maven.tar.gz

ENV MAVEN_HOME=/opt/maven
ENV PATH=$MAVEN_HOME/bin:$PATH
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Phoenix surefire defaults expect plenty of file descriptors.
RUN ulimit -n 65536 || true

WORKDIR /work

ENTRYPOINT ["/usr/bin/tini", "--", "/work/docker/it-runner-entrypoint.sh"]
