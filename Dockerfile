FROM ubuntu

RUN apt-get update && apt-get install make
RUN git clone https://github.build.ge.com/gridos-data-fabric/DuckDB && cd DuckDB && make


