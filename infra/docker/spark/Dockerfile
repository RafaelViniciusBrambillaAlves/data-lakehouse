FROM apache/spark:3.4.1

USER root

# Instala utilitários
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Diretório para JARs extras
RUN mkdir -p /opt/spark/jars

# Download dos JARs necessários
RUN curl -fL -o /opt/spark/jars/delta-core_2.12-2.4.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -fL -o /opt/spark/jars/delta-storage-2.4.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fL -o /opt/spark/jars/postgresql-42.6.0.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Cria diretório home do spark
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

# Instala pacotes Python
RUN pip install --no-cache-dir notebook==6.5.6 python-dotenv psycopg2-binary pyspark==3.4.1 delta-spark==2.4.0

# Alterna para usuário spark
USER spark
WORKDIR /home/spark