FROM metabase/metabase:v0.50.27

RUN mkdir -p /plugins

RUN curl -L https://github.com/starburstdata/metabase-driver/releases/download/6.1.0/starburst-6.1.0.metabase-driver.jar \
    -o /plugins/starburst.metabase-driver.jar