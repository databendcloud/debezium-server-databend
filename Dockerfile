FROM eclipse-temurin:11-jdk as builder
LABEL org.opencontainers.image.source=https://github.com/databendcloud/debezium-server-databend
LABEL org.opencontainers.image.description="Debezium server databend container image"
LABEL org.opencontainers.image.licenses=Apache
RUN apt-get -qq update && apt-get -qq install maven unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet
RUN unzip /app/debezium-server-databend-dist/target/debezium-server-databend-dist*.zip -d appdist

FROM eclipse-temurin:11-jre
COPY --from=builder /app/appdist/debezium-server-databend/ /app/

WORKDIR /app
EXPOSE 8080 8083
VOLUME ["/app/conf", "/app/data"]

ENTRYPOINT ["/app/run.sh"]