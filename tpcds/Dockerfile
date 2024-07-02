FROM rust:1-slim-buster

RUN apt update && apt install -y zip gcc make flex bison byacc git

# TPC-DS generator
COPY tpc-ds-tool.zip .
RUN unzip tpc-ds-tool.zip
WORKDIR /DSGen-software-code-3.2.0rc1/tools
RUN make

# tpctools crate
RUN cargo install tpctools

ADD gen.sh .