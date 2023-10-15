FROM ubuntu
WORKDIR /build
COPY . ./
RUN apt update && apt install -y make
RUN make
