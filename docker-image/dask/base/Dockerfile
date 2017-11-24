# Ubuntu 16.04 (Xenial)
FROM ubuntu:16.04

# set version of python required for thunderbolt application
ENV AZTK_PYTHON_VERSION=3.5.4

# set up env vars for pyenv
ENV HOME /
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

RUN apt-get clean \
    && apt-get update -y \
    # install dependency packages
    && apt-get install -y --no-install-recommends \
       make \
       build-essential \
       zlib1g-dev \
       libssl-dev \
       libbz2-dev \
       libreadline-dev \
       libsqlite3-dev \
       wget \
       curl \
       llvm \
       git \
       libncurses5-dev \
       libncursesw5-dev \
       xz-utils \
       tk-dev \
    && apt-get update -y \
    && apt-get install -y --no-install-recommends software-properties-common \
    # download pyenv
    && git clone https://github.com/pyenv/pyenv.git .pyenv \
    && git clone https://github.com/pyenv/pyenv-virtualenv.git ~/.pyenv/plugins/pyenv-virtualenv \
    # install & setup pyenv
    && eval "$(pyenv init -)" \
    && echo 'eval "$(pyenv init -)"' >> ~/.bashrc \
    # install aztk required python version 
    && pyenv install -f $AZTK_PYTHON_VERSION \
    && pyenv global $AZTK_PYTHON_VERSION 

RUN pip install dask[complete] distributed --upgrade

# unicode environment variables
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

CMD ["/bin/bash"]
