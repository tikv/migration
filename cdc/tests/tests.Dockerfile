# Copy from deployments/tikv-cdc/docker/integration-test.Dockerfile,
# but use local files other than COPY
# amd64 only
FROM amd64/centos:centos7 as downloader

USER root
WORKDIR /root/download

# Download go into /usr/local dir.
# Get the SHA256 from https://golang.org/dl/.
ENV GOLANG_VERSION 1.21.4
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go${GOLANG_VERSION}.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 73cac0215254d0c7d1241fa40837851f3b9a8a742d0b54714cbdfb3feaf8f0af
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
	psmisc \
	mysql \
	python3 \
	lsof

RUN wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y epel-release-latest-7.noarch.rpm
RUN yum --enablerepo=epel install -y s3cmd jq

COPY --from=downloader /usr/local/go /usr/local/go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

WORKDIR /cdc

ENTRYPOINT ["/bin/bash"]
