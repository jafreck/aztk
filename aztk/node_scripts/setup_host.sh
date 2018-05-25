#!/bin/bash

# Entry point for the start task. It will install all dependencies and start docker.
# Usage:
# setup_host.sh [container_name] [docker_repo_name]
set -e

export AZTK_WORKING_DIR=/mnt/batch/tasks/startup/wd
export PYTHONUNBUFFERED=TRUE

container_name=$1
docker_repo_name=$2

echo "Installing pre-reqs"
time(
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

    packages=(
        apt-transport-https
        curl
        ca-certificates
        software-properties-common
        python3-pip
        python3-venv
        docker-ce
    )

    echo "running apt-get install -y --no-install-recommends \"${packages[@]}\""
    apt-get -y update &&
    apt-get install -y --no-install-recommends "${packages[@]}"

    if [ $AZTK_GPU_ENABLED == "true" ]; then
        apt-get install -y nvidia-384 nvidia-modprobe
        wget -P /tmp https://github.com/NVIDIA/nvidia-docker/releases/download/v1.0.1/nvidia-docker_1.0.1-1_amd64.deb
        sudo dpkg -i /tmp/nvidia-docker*.deb && rm /tmp/nvidia-docker*.deb
    fi
) 2>&1

echo "Finished installing pre-reqs"

# set hostname in /etc/hosts if dns cannot resolve
if ! host $HOSTNAME ; then
    echo $(hostname -I | awk '{print $1}') $HOSTNAME >> /etc/hosts
fi

# Install docker-compose
echo "Installing Docker-Componse"
time(
    sudo curl -L https://github.com/docker/compose/releases/download/1.19.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
) 2>&1
echo "Finished installing Docker-Compose"

if [ -z "$DOCKER_USERNAME" ]; then
    echo "No Credentials provided. No need to login to dockerhub"
else
    echo "Docker credentials provided. Login in."
    docker login $DOCKER_ENDPOINT --username $DOCKER_USERNAME --password $DOCKER_PASSWORD
fi

echo "Pulling $docker_repo_name"
(time docker pull $docker_repo_name) 2>&1
echo "Finished pulling $docker_repo_name"

# Unzip resource files and set permissions
chmod 777 $AZTK_WORKING_DIR/aztk/node_scripts/docker_main.sh

# Check docker is running
docker info > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "UNKNOWN - Unable to talk to the docker daemon"
  exit 3
fi

echo "Node python version:"
python3 --version

# set up aztk python environment
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
python3 -m pip install pipenv
mkdir -p $AZTK_WORKING_DIR/.aztk-env
cp $AZTK_WORKING_DIR/aztk/node_scripts/Pipfile $AZTK_WORKING_DIR/.aztk-env
cp $AZTK_WORKING_DIR/aztk/node_scripts/Pipfile.lock $AZTK_WORKING_DIR/.aztk-env
cd $AZTK_WORKING_DIR/.aztk-env
export PIPENV_VENV_IN_PROJECT=true

# Install python dependencies
echo "Installing python dependencies"
time(
    pipenv install --python /usr/bin/python3.5m
    pipenv run pip install --upgrade setuptools wheel #TODO: add pip when pipenv is compatible with pip10
) 2>&1
export PYTHONPATH=$PYTHONPATH:$AZTK_WORKING_DIR
echo "Finished installing python dependencies"

echo "Running docker container"
time(
    # If the container already exists just restart. Otherwise create it
    if [ "$(docker ps -a -q -f name=$container_name)" ]; then
        echo "Docker container is already setup. Restarting it."
        docker restart $container_name
    else
        echo "Creating docker container."

        echo "Running setup python script"
        $AZTK_WORKING_DIR/.aztk-env/.venv/bin/python $(dirname $0)/main.py setup-node $docker_repo_name

        # wait until container is running
        until [ "`/usr/bin/docker inspect -f {{.State.Running}} $container_name`"=="true" ]; do
            sleep 0.1;
        done;

        # wait until container setup is complete
        echo "Waiting for spark docker container to setup."
        docker exec spark /bin/bash -c '$AZTK_WORKING_DIR/.aztk-env/.venv/bin/python $AZTK_WORKING_DIR/aztk/node_scripts/wait_until_setup_complete.py'

        # Setup symbolic link for the docker logs
        docker_log=$(docker inspect --format='{{.LogPath}}' $container_name)
        mkdir -p $AZ_BATCH_TASK_WORKING_DIR/logs
        ln -s $docker_log $AZ_BATCH_TASK_WORKING_DIR/logs/docker.log
    fi
) 2>&1
echo "Finished running docker container"
