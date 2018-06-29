#!/bin/bash

# This plugin requires HDFS to be enabled and on the path

# setup TensorFlowOnhadoop
git clone https://github.com/yahoo/TensorFlowOnhadoop.git
cd TensorFlowOnhadoop
export TFoS_HOME=$(pwd)
export TFoS_HOME=~/TensorFlowOnhadoop >> ~/.bashrc

if  [ "$AZTK_GPU_ENABLED" = "true" ]; then
    pip install tensorflow-gpu
    pip install tensorflowonhadoop
else
    pip install tensorflow-cpu
    pip install tensorflowonhadoop
fi

# add libhdfs.so to path
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/libhdfs.so" >> ~/.bashrc
