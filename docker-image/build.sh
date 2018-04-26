#/bin/bash

# setup docker to build on /mnt instead of /var/lib/docker
echo '{
    "graph": "/mnt",
    "storage-driver": "overlay"
}' > /etc/docker/daemon.json

service docker restart


# base 2.1.0
docker build docker-image/base/spark2.1.0/ --tag aztk/spark:v0.1.0-spark2.1.0-base > out/base-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-base

# base 2.2.0
docker build docker-image/base/spark2.2.0/ --tag aztk/spark:v0.1.0-spark2.2.0-base > out/base-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-base

# base 2.3.0
docker build docker-image/base/spark2.3.0/ --tag aztk/spark:v0.1.0-spark2.3.0-base > out/base-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-base

# miniconda-base 2.1.0
docker build docker-image/miniconda/spark2.1.0/base/ --tag aztk/spark:v0.1.0-spark2.1.0-miniconda-base > out/miniconda-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-miniconda-base

# miniconda-base 2.2.0
docker build docker-image/miniconda/spark2.2.0/base --tag aztk/spark:v0.1.0-spark2.2.0-miniconda-base > out/miniconda-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-miniconda-base

# miniconda-base 2.3.0
docker build docker-image/miniconda/spark2.3.0/base/ --tag aztk/spark:v0.1.0-spark2.3.0-miniconda-base > out/miniconda-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-miniconda-base

# anaconda-base 2.1.0
docker build docker-image/anaconda/spark2.1.0/base/ --tag aztk/spark:v0.1.0-spark2.1.0-anaconda-base > out/anaconda-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-anaconda-base

# anaconda-base 2.2.0
docker build docker-image/anaconda/spark2.2.0/base/ --tag aztk/spark:v0.1.0-spark2.2.0-anaconda-base > out/anaconda-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-anaconda-base

# anaconda-base 2.3.0
docker build docker-image/anaconda/spark2.3.0/base/ --tag aztk/spark:v0.1.0-spark2.3.0-anaconda-base > out/anaconda-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-anaconda-base

# r-base 2.1.0
docker build docker-image/r/spark2.1.0/base/ --tag aztk/spark:v0.1.0-spark2.1.0-r-base > out/r-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-r-base

# r-base 2.2.0
docker build docker-image/r/spark2.2.0/base/ --tag aztk/spark:v0.1.0-spark2.2.0-r-base > out/r-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-r-base

# r-base 2.3.0
docker build docker-image/r/spark2.3.0/base/ --tag aztk/spark:v0.1.0-spark2.3.0-r-base > out/r-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-r-base

##################
#      GPU       #
##################

# gpu 2.1.0
docker build docker-image/gpu/spark2.1.0/ --tag aztk/spark:v0.1.0-spark2.1.0-gpu > out/gpu-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-gpu

# gpu 2.2.0
docker build docker-image/gpu/spark2.2.0/ --tag aztk/spark:v0.1.0-spark2.2.0-gpu > out/gpu-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-gpu

# gpu 2.3.0
docker build docker-image/gpu/spark2.3.0/ --tag aztk/spark:v0.1.0-spark2.3.0-gpu > out/gpu-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-gpu

# miniconda-gpu 2.1.0
docker build docker-image/miniconda/spark2.1.0/gpu/ --tag aztk/spark:v0.1.0-spark2.1.0-miniconda-gpu > out/miniconda-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-miniconda-gpu

# miniconda-gpu 2.2.0
docker build docker-image/miniconda/spark2.2.0/gpu --tag aztk/spark:v0.1.0-spark2.2.0-miniconda-gpu > out/miniconda-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-miniconda-gpu

# miniconda-gpu 2.3.0
docker build docker-image/miniconda/spark2.3.0/gpu/ --tag aztk/spark:v0.1.0-spark2.3.0-miniconda-gpu > out/miniconda-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-miniconda-gpu

# anaconda-gpu 2.1.0
docker build docker-image/anaconda/spark2.1.0/gpu/ --tag aztk/spark:v0.1.0-spark2.1.0-anaconda-gpu > out/anaconda-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-anaconda-gpu

# anaconda-gpu 2.2.0
docker build docker-image/anaconda/spark2.2.0/gpu/ --tag aztk/spark:v0.1.0-spark2.2.0-anaconda-gpu > out/anaconda-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-anaconda-gpu

# anaconda-gpu 2.3.0
docker build docker-image/anaconda/spark2.3.0/gpu/ --tag aztk/spark:v0.1.0-spark2.3.0-anaconda-gpu > out/anaconda-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-anaconda-gpu

# r-gpu 2.1.0
docker build docker-image/r/spark2.1.0/gpu/ --tag aztk/spark:v0.1.0-spark2.1.0-r-gpu > out/r-spark2.1.0.out &&
docker push aztk/spark:v0.1.0-spark2.1.0-r-gpu

# r-gpu 2.2.0
docker build docker-image/r/spark2.2.0/gpu/ --tag aztk/spark:v0.1.0-spark2.2.0-r-gpu > out/r-spark2.2.0.out &&
docker push aztk/spark:v0.1.0-spark2.2.0-r-gpu

# r-gpu 2.3.0
docker build docker-image/r/spark2.3.0/gpu/ --tag aztk/spark:v0.1.0-spark2.3.0-r-gpu > out/r-spark2.3.0.out &&
docker push aztk/spark:v0.1.0-spark2.3.0-r-gpu
