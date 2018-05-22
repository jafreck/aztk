#!/bin/bash

echo "Pulling GATK Docker image from Dockerhub..."

docker pull aztk/staging:gatk4

echo "Running GATK Docker container"

docker run -td --name gatk aztk/staging:gatk4

echo "Finished setting up GATK Docker container"
