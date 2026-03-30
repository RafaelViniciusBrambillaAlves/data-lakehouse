    FROM apache/spark:3.4.1

    USER root

    RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

    # O diretório correto para JARs extras na imagem apache/spark é /opt/spark/jars
    # Verificamos que ele existe (já existe por padrão, mas garantimos)
    RUN mkdir -p /opt/spark/jars

    # Delta Lake 2.4.0 (compatível com Spark 3.4.x) + S3 + Postgres
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

    # Valida que os JARs foram baixados corretamente (tamanho > 0)
    RUN for jar in \
            delta-core_2.12-2.4.0.jar \
            delta-storage-2.4.0.jar \
            hadoop-aws-3.3.4.jar \
            aws-java-sdk-bundle-1.12.262.jar \
            postgresql-42.6.0.jar; do \
        [ -s /opt/spark/jars/$jar ] || (echo "ERRO: $jar está vazio ou ausente" && exit 1); \
        done

    RUN pip install --no-cache-dir python-dotenv psycopg2-binary

    USER spark