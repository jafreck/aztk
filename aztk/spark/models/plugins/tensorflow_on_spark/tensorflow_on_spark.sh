#!/bin/bash


# setup TensorFlowOnSpark
git clone https://github.com/yahoo/TensorFlowOnSpark.git
cd TensorFlowOnSpark
export TFoS_HOME=$(pwd)

pip install tensorflow-cpu
pip install tensorflowonspark
