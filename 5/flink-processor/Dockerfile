FROM openjdk:8-jre-slim
COPY target/flink-kafka2postgres-1.0-SNAPSHOT.jar /app/flink-kafka2postgres.jar
COPY wait-for-it.sh /app/wait-for-it.sh
RUN chmod +x /app/wait-for-it.sh


ENTRYPOINT ["/app/wait-for-it.sh", "kafka:9092", "--", "java", "-cp", "/app/flink-kafka2postgres.jar", "aisdata.Main"]
