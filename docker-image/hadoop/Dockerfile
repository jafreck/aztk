FROM ubuntu:16.04
ARG HADOOP_VERSION=3.1.0

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        openjdk-8-jdk \
        supervisor \
    && apt-get autoclean -y \
    && rm -rf /var/lib/apt/lists/*

RUN set -x \
    && curl -fSL "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" | tar -xvz -C /opt/

RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop
RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs
RUN mkdir -p /var/run/supervisor

ENV HADOOP_CONF_DIR=/etc/hadoop
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH $HADOOP_HOME/bin/:$PATH

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY supervisord.conf /etc/supervisord.conf

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

CMD ["/bin/bash"]
