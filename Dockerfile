FROM ubuntu

RUN apt-get -y update && apt-get -y install make && apt-get -y install git
RUN make


