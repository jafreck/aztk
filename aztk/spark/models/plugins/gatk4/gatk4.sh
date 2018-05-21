#!/bin/bash

echo "Pulling GATK Docker image from Dockerhub..."

docker pull broadinstitute/gatk:4.0.4.0

echo "Running GATK Docker container"

docker run broadinstitute/gatk:4.0.4.0 --name gatk

echo "Finished setting up GATK Docker container"
