FROM rust:alpine as builder

RUN apk add build-base libressl-dev
WORKDIR "/usr/share/bytebeam/uplink"

COPY uplink/ uplink
COPY storage/ storage
COPY tools/ tools
COPY Cargo.* .
COPY .git/ .git

RUN mkdir -p /usr/share/bytebeam/uplink/bin
RUN cargo build --release
RUN cp target/release/uplink /usr/share/bytebeam/uplink/bin/

###################################################################################################

FROM alpine:latest

RUN apk add runit bash curl coreutils
WORKDIR "/usr/share/bytebeam/uplink"

RUN mkdir -p /usr/share/bytebeam/uplink
COPY --from=builder /usr/share/bytebeam/uplink/bin /usr/bin
COPY runit/ /etc/runit

CMD ["/usr/bin/runsvdir", "/etc/runit"]

COPY paths/ paths
COPY simulator.sh .

