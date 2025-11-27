FROM rust:1-slim-bullseye

RUN apt update && apt install -y zip gcc make flex bison byacc git

# TPC-DS generator
COPY tpc-ds-tool.zip .
RUN unzip tpc-ds-tool.zip

# Find the actual extracted directory and create a consistent symlink
RUN ls -la && \
    EXTRACTED_DIR=$(find . -maxdepth 1 -name "DSGen-software-code*" -type d | head -1) && \
    echo "Found directory: $EXTRACTED_DIR" && \
    ln -sf "$EXTRACTED_DIR" /dsgen-tools

WORKDIR /dsgen-tools/tools

# Fix bad UTF-8 char
RUN iconv -f ISO-8859-14 -t UTF-8 tpcds.dst > tpcds.dst2
RUN mv tpcds.dst2 tpcds.dst

# compile with flags to handle multiple definitions
RUN make CC="gcc -fcommon"

# Copy and make gen.sh executable
COPY gen.sh /usr/local/bin/gen.sh
RUN chmod +x /usr/local/bin/gen.sh

# Set working directory to root for script execution
WORKDIR /