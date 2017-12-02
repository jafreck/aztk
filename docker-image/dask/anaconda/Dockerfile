FROM jafreck/aztk:dask

# modify these ARGs on build time to specify your desired versions of Spark/Hadoop
ARG ANACONDA_VERSION=anaconda3-5.0.0

# install user specificed version of anaconda
RUN pyenv install -f $ANACONDA_VERSION \
    && pyenv global $ANACONDA_VERSION 

# set env vars
ENV USER_PYTHON_VERSION $ANACONDA_VERSION

RUN conda install -y dask \
    && conda install -y bokeh -c bokeh

CMD ["/bin/bash"]
