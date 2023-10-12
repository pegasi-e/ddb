FROM ubuntu

RUN apt-get update && apt-get install make
RUN make

