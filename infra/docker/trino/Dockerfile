FROM trinodb/trino:435
 
# Copy catalog configurations
COPY infra/trino/catalog/ /etc/trino/catalog/
 
# Copy node and JVM config
COPY infra/trino/config/config.properties /etc/trino/config.properties
COPY infra/trino/config/jvm.config         /etc/trino/jvm.config
COPY infra/trino/config/log.properties     /etc/trino/log.properties
