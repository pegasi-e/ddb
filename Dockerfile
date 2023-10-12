FROM ubuntu

RUN apt-get update && apt-get install make && apt-get install git
RUN git clone https://github.build.ge.com/gridos-data-fabric/DuckDB && cd DuckDB && make


