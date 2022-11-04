# Copy from deployments/tikv-cdc/docker/integration-test.Dockerfile,
# but use local files other than COPY
# amd64 only
FROM amd64/centos:centos7 as downloader

USER root
WORKDIR /root/download

COPY ./scripts/download-integration-test-binaries.sh .
# Download all binaries into bin dir.
RUN ./download-integration-test-binaries.sh master
RUN ls ./bin

# Download go into /usr/local dir.
ENV GOLANG_VERSION 1.18.5
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go${GOLANG_VERSION}.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 9e5de37f9c49942c601b191ac5fba404b868bfc21d446d6960acc12283d6e5f2
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

FROM amd64/centos:centos7

USER root
WORKDIR /root

# Installing dependencies.
RUN yum install -y \
	git \
	bash-completion \
	wget \
    which \
	gcc \
	make \
    curl \
    tar \
    musl-dev \
    psmisc \
	mysql \
    jq
RUN wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y epel-release-latest-7.noarch.rpm
RUN yum --enablerepo=epel install -y s3cmd

COPY --from=downloader /usr/local/go /usr/local/go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

WORKDIR /cdc

COPY --from=downloader /root/download/bin/* ./scripts/bin/

ENTRYPOINT ["/bin/bash"]
