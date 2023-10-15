FROM ubuntu
WORKDIR /build
COPY . ./
RUN ls
RUN make
