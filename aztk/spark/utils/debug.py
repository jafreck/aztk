"""
    Diagnostic program that runs on each node in the cluster
    This program must be run with sudo
"""
import os
import io
import json
import socket
from subprocess import check_output, STDOUT, CalledProcessError
from zipfile import ZipFile, ZIP_DEFLATED
import tarfile
import docker


def main():
    zipf = create_zip_archive()

    # general node diagnostics
    zipf.writestr("hostname.txt", data=get_hostname())
    zipf.writestr("df.txt", data=get_disk_free())

    # docker container diagnostics
    docker_client = docker.from_env()
    for filename, data in get_docker_diagnostics(docker_client):
        zipf.writestr(filename, data=data)

    zipf.close()


def create_zip_archive():
    zip_file_path = "/tmp/debug.zip"
    return ZipFile(zip_file_path, "w", ZIP_DEFLATED)


def get_hostname():
    return socket.gethostname()


def cmd_check_output(cmd):
    try:
        output = check_output(cmd, shell=True, stderr=STDOUT)
    except CalledProcessError as e:
        return "CMD: {0}\n"\
               "returncode: {1}"\
               "output: {2}".format(e.cmd, e.returncode, e.output)
    else:
        return output


def get_disk_free():
    return cmd_check_output("df -h")


def get_docker_diagnostics(docker_client):
    '''
        returns list of tuples (filename, data) to be written in the zip
    '''
    output = []
    output.append(get_docker_images(docker_client))
    logs = get_docker_containers(docker_client)
    for item in logs:
        output.append(item)

    return output


def get_docker_images(docker_client):
    output = ""
    try:
        images = docker_client.images.list()
        for image in images:
            output += json.dumps(image.attrs, sort_keys=True, indent=4)
        return ("docker-images.txt", output)
    except docker.errors.APIerror as e:
        return ("docker-images.err", e.__str__())


def get_docker_containers(docker_client):
    container_attrs = ""
    logs = []
    try:
        containers = docker_client.containers.list()
        for container in containers:
            container_attrs += json.dumps(container.attrs, sort_keys=True, indent=4)
            # get docker container logs
            logs.append((container.name + "/docker.log", container.logs()))
            logs.append(get_docker_process_status(container))
            if container.name == "spark": #TODO: find a more robust way to get specific info off specific containers
                logs.append(get_container_aztk_script(container))
                logs.append(get_spark_logs(container))

        logs.append(("docker-containers.txt", container_attrs))
        return logs
    except docker.errors.APIerror as e:
        return [("docker-containers.err", e.__str__())]


def get_docker_process_status(container):
    try:
        exit_code, output = container.exec_run("ps -auxw", tty=True, privileged=True)
        out_file_name = container.name + "/ps_aux.txt"
        if exit_code == 0:
            return (out_file_name, output)
        else:
            return (out_file_name, "exit_code: {0}\n{1}".format(exit_code, output))
    except docker.errors.APIerror as e:
        return (container.name + "ps_aux.err", e.__str__())


def get_container_aztk_script(container):
    aztk_path = "/mnt/batch/tasks/startup/wd"
    try:
        stream, _ = container.get_archive(aztk_path) # second item is stat info
        data = b''.join([item for item in stream])
        return (container.name + "/" + "aztk-scripts.tar", data)
    except docker.errors.APIError as e:
        return (container.name + "/" + "aztk-scripts.err", e.__str__())


def get_spark_logs(container):
    spark_logs_path = "/home/spark-current/logs"
    data = b''
    try:
        stream, _ = container.get_archive(spark_logs_path) # second item is stat info
        data = b''.join([item for item in stream])
        return (container.name + "/" + "spark-logs.tar", data)
    except docker.errors.APIError as e:
        return (container.name + "/" + "spark-logs.err", e.__str__())


if __name__ == "__main__":
    main()
