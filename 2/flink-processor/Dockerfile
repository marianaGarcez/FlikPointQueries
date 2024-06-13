FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y git curl gnupg build-essential tree vim cmake postgresql-server-dev-15 libproj-dev libjson-c-dev libgsl-dev libgeos-dev postgis \
    && export GNUPGHOME="$(mktemp -d)" \
    && curl -fL https://apt.corretto.aws/corretto.key | gpg --batch --import \
    && gpg --batch --export '6DC3636DAE534049C8B94623A122542AB04F24E3' > /usr/share/keyrings/corretto.gpg \
    && rm -r "$GNUPGHOME" \
    && unset GNUPGHOME \
    && echo "deb [signed-by=/usr/share/keyrings/corretto.gpg] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list \
    && apt-get update \
    && apt-get install -y java-21-amazon-corretto-jdk

# Install Maven
RUN apt-get update && \
    apt-get install -y maven

COPY target/flink-kafka2postgres-1.0-SNAPSHOT.jar /app/flink-kafka2postgres.jar
COPY  /jar/JMEOS.jar /usr/local/jar/JMEOS.jar

COPY wait-for-it.sh /app/wait-for-it.sh

RUN chmod +x /app/wait-for-it.sh

ENTRYPOINT ["/app/wait-for-it.sh", "kafka:9092", "--", "java", "-cp", "/app/flink-kafka2postgres.jar", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "aisdata.Main"]