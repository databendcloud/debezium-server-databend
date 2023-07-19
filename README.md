# debezium-server-databend
This project could be used to receive database CDC changes from debezium server and send it to [databend](https://github.com/datafuselabs/databend) table.It's in realtime and  started independently without the need for auxiliary streaming platforms such as Kafka, Flink and Spark. 

# debezium databend consumer
The detail introduction docs available in [docs page](./docs/docs.md)

# Install from source

- Requirements:
    - JDK 11
    - Maven
- Clone from repo: `git clone https://github.com/databendcloud/debezium-server-databend.git`
- From the root of the project:
    - Build and package debezium server: `mvn -Passembly -Dmaven.test.skip package`
    - After building, unzip your server
      distribution: `unzip debezium-server-databend-dist/target/debezium-server-databend-dist*.zip -d databendDist`
    - cd into unzipped folder: `cd databendDist`
    - Create `application.properties` file and config it: `nano conf/application.properties`, you can check the example
      configuration
      in [application.properties.example](debezium-server-databend-sink/src/main/resources/conf/application.properties.example)
    - Run the server using provided script: `bash run.sh`
    - The debezium server with databend will be started

# Contributing

You are warmly welcome to hack on debezium-server-databend. We have prepared a guide [CONTRIBUTING.md](./CONTRIBUTING.md).
