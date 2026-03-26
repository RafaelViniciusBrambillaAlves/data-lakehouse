FROM apache/hive:4.0.0

USER root

RUN apt-get update && apt-get install -y curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Driver JDBC PostgreSQL
RUN curl -fsSL -o /opt/hive/lib/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    && [ -s /opt/hive/lib/postgresql-42.7.3.jar ] \
    || (echo "ERRO: postgresql jar ausente" && exit 1)

# AWS SDK
RUN curl -fsSL -o /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && [ -s /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar ] \
    || (echo "ERRO: aws-sdk jar ausente" && exit 1)

# hadoop-aws que já vem na imagem (3.3.6) via symlink — evita conflito de classloader
RUN ln -sf /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar \
           /opt/hive/lib/hadoop-aws-3.3.6.jar

USER hive