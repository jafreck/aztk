#!/bin/bash

# This custom script has been tested to work on the following docker images:
#  - aztk/python:hadoop2.2.0-python3.6.2-base
#  - aztk/python:hadoop2.2.0-python3.6.2-gpu
#  - aztk/python:hadoop2.1.0-python3.6.2-base
#  - aztk/python:hadoop2.1.0-python3.6.2-gpu

if  [ "$AZTK_IS_MASTER" = "true" ]; then
    conda install -c conda-force jupyterlab

    PYhadoop_DRIVER_PYTHON="/opt/conda/bin/jupyter"
    JUPYTER_KERNELS="/opt/conda/share/jupyter/kernels"

    # disable password/token on jupyter notebook
    jupyter lab --generate-config --allow-root
    JUPYTER_CONFIG='/root/.jupyter/jupyter_notebook_config.py'
    echo >> $JUPYTER_CONFIG
    echo -e 'c.NotebookApp.token=""' >> $JUPYTER_CONFIG
    echo -e 'c.NotebookApp.password=""' >> $JUPYTER_CONFIG

    # get master ip
    MASTER_IP=$(hostname -i)

    # remove existing kernels
    rm -rf $JUPYTER_KERNELS/*

    # set up jupyter to use pyhadoop
    mkdir $JUPYTER_KERNELS/pyhadoop
    touch $JUPYTER_KERNELS/pyhadoop/kernel.json
    cat << EOF > $JUPYTER_KERNELS/pyhadoop/kernel.json
{
    "display_name": "Pyhadoop",
    "language": "python",
    "argv": [
        "python",
        "-m",
        "ipykernel",
        "-f",
        "{connection_file}"
    ],
    "env": {
        "hadoop_HOME": "$hadoop_HOME",
        "PYhadoop_PYTHON": "python",
        "PYHADOOP_SUBMIT_ARGS": "--master hadoop://$MASTER_IP:7077 pyhadoop-shell"
    }
}
EOF

    # start jupyter notebook from /mnt - this is where we recommend you put your azure files mount point as well
    cd /mnt
    (PYhadoop_DRIVER_PYTHON=$PYhadoop_DRIVER_PYTHON PYhadoop_DRIVER_PYTHON_OPTS="lab --no-browser --port=8889 --allow-root" pyhadoop &)
fi


