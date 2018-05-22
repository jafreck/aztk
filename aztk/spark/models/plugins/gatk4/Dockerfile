FROM ubuntu:16.04

ARG GATK_VERSION="4.0.4.0"
ARG MINICONDA_VERISON=Miniconda3-4.4.10

ENV JAVA_LIBRARY_PATH /usr/lib/jni
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bzip2 \
        ca-certificates \
        curl \
        git  \
        openjdk-8-jdk \
        software-properties-common \
        unzip \
        vim \
        wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -o /tmp/gatk-${GATK_VERSION}.zip -fSL "https://github.com/broadinstitute/gatk/releases/download/${GATK_VERSION}/gatk-${GATK_VERSION}.zip" \
    && unzip /tmp/gatk-${GATK_VERSION}.zip -d /opt/ \
    && ln -s /opt/gatk-${GATK_VERSION} /opt/gatk \
    && echo "export PATH=\$PATH:/opt/gatk" >> ~/.gatkbashrc

RUN wget --quiet https://repo.continuum.io/miniconda/${MINICONDA_VERISON}-Linux-x86_64.sh -O ~/miniconda.sh \
    && /bin/bash ~/miniconda.sh -b -p /opt/conda \
    && rm ~/miniconda.sh \
    && /opt/conda/bin/conda clean -tipsy \
    && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh \
    && echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.gatkbashrc

RUN . /opt/conda/etc/profile.d/conda.sh \
    && conda env create --file /opt/gatk/gatkcondaenv.yml --name gatk \
    && echo "conda activate gatk" >> ~/.gatkbashrc


CMD ["/bin/bash", "--init-file", "/root/.gatkbashrc"]

# sudo docker exec gatk4 /bin/bash -c 'source /root/.bashrc; echo $PATH;'
