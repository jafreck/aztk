#!/bin/bash


# setup TensorFlowOnSpark
git clone https://github.com/yahoo/TensorFlowOnSpark.git
cd TensorFlowOnSpark
export TFoS_HOME=$(pwd)
export TFoS_HOME=~/TensorFlowOnSpark >> ~/.bashrc

pip install tensorflow-cpu
pip install tensorflowonspark
