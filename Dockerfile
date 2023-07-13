FROM ubuntu:18.04 AS base
SHELL ["/bin/bash", "-c"]

RUN echo "APT::Acquire::Retries \"3\";" > /etc/apt/apt.conf.d/80-retries

RUN apt-get upgrade
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y curl vim libssl-dev pkg-config

RUN mkdir -p /etc/bytebeam /usr/share/bytebeam

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

#CMD ["/usr/bin/runsvdir", "/etc/runit"]
#COPY runit/ /etc/runit
#RUN rm -rf /etc/runit/runsvdir

WORKDIR "/usr/share/bytebeam/uplink"

#####################################################################################

FROM base as staging

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup
RUN chmod +x /tmp/rustup
RUN /tmp/rustup -y
RUN source $HOME/.cargo/env

COPY . /usr/share/bytebeam/uplink

RUN mkdir -p /usr/share/bytebeam/uplink/bin
WORKDIR /usr/share/bytebeam/uplink/tools/simulator
RUN $HOME/.cargo/bin/cargo build --release
RUN cp target/release/simulator /usr/share/bytebeam/uplink/bin/
WORKDIR /usr/share/bytebeam/uplink
RUN $HOME/.cargo/bin/cargo build --release
RUN cp target/release/uplink /usr/share/bytebeam/uplink/bin/

###################################################################################################

FROM base AS production

RUN mkdir -p /usr/share/bytebeam/uplink
RUN mkdir -p /usr/share/bytebeam/uplink/shared
#RUN mkdir -P /usr/share/bytebeam/uplink/bin
COPY --from=staging /usr/share/bytebeam/uplink/bin /usr/bin
COPY --from=staging /usr/share/bytebeam/uplink/paths /usr/share/bytebeam/uplink/paths
COPY --from=staging /usr/share/bytebeam/uplink/simulator.sh /usr/share/bytebeam/uplink
COPY --from=staging /usr/share/bytebeam/uplink/bin /usr/share/bytebeam/uplink
CMD uplink -h
#CMD  cp /usr/share/bytebeam/uplink/uplink /usr/share/bytebeam/uplink/shared/uplink
