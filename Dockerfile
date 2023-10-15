FROM ubuntu
WORKDIR /build
COPY . ./
RUN apt update && apt install -y make && apt install -y build-essential clang cmake
RUN make
