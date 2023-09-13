FROM ubuntu:18.04 AS base
SHELL ["/bin/bash", "-c"]

RUN echo "APT::Acquire::Retries \"3\";" > /etc/apt/apt.conf.d/80-retries

RUN apt-get upgrade
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y curl runit vim libssl-dev pkg-config

RUN mkdir -p /etc/bytebeam /usr/share/bytebeam

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

WORKDIR "/usr/share/bytebeam/uplink"

#####################################################################################

FROM base as builder

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup
RUN chmod +x /tmp/rustup
RUN /tmp/rustup -y
RUN source $HOME/.cargo/env

COPY uplink/ /usr/share/bytebeam/uplink/uplink
COPY storage/ /usr/share/bytebeam/uplink/storage
COPY Cargo.* /usr/share/bytebeam/uplink/
COPY .git/ /usr/share/bytebeam/uplink/.git

RUN mkdir -p /usr/share/bytebeam/uplink/bin
RUN $HOME/.cargo/bin/cargo build --release
RUN cp target/release/uplink /usr/share/bytebeam/uplink/bin/

###################################################################################################

FROM base AS simulator

RUN mkdir -p /usr/share/bytebeam/uplink
COPY --from=builder /usr/share/bytebeam/uplink/bin /usr/bin

CMD ["/usr/bin/runsvdir", "/etc/runit"]
COPY runit/ /etc/runit
RUN rm -rf /etc/runit/runsvdir

COPY paths/ /usr/share/bytebeam/uplink/paths
COPY simulator.sh /usr/share/bytebeam/uplink/