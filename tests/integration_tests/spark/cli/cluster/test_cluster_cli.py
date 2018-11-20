import subprocess
from contextlib import contextmanager

import pytest
from tests.integration_tests.spark.get_test_suffix import get_test_suffix

base_cmd = ["aztk", "spark", "cluster"]
base_cluster_id = get_test_suffix("c")


def execute_command(cmd):
    print(cmd)
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    proc.wait()
    return proc


@contextmanager
def create_cli_cluster(id, *args, wait=True, **kwargs):
    print("running create_cli_cluster")
    cmd = base_cmd.copy()
    cmd.extend(["create", "--id", id])
    if wait:
        cmd.extend(["--wait"])
    for key, value in kwargs.items():
        cmd.extend(["--{}".format(key), value])
    for arg in args:
        cmd.extend(arg)
    process = execute_command(cmd)
    try:
        yield
    finally:
        cmd = base_cmd.copy()
        cmd.extend(["delete", "--id", id, "--force"])
        process = execute_command(cmd)


def test_cluster_create_no_args():
    cmd = base_cmd.copy()
    cmd.append("create")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_add_user_no_args():
    cmd = base_cmd.copy()
    cmd.append("add-user")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_delete_no_args():
    cmd = base_cmd.copy()
    cmd.append("delete")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_get_no_args():
    cmd = base_cmd.copy()
    cmd.append("get")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_list_no_args():
    cmd = base_cmd.copy()
    cmd.append("list")
    process = execute_command(cmd)
    assert process.returncode == 0


def test_cluster_app_logs_no_args():
    cmd = base_cmd.copy()
    cmd.append("app-logs")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_ssh_no_args():
    cmd = base_cmd.copy()
    cmd.append("ssh")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_submit_no_args():
    cmd = base_cmd.copy()
    cmd.append("submit")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_run_no_args():
    cmd = base_cmd.copy()
    cmd.append("run")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_copy_no_args():
    cmd = base_cmd.copy()
    cmd.append("copy")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_debug_no_args():
    cmd = base_cmd.copy()
    cmd.append("debug")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_create_success_basic():
    cmd = base_cmd.copy()
    cmd.append("create")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_add_user_success_basic():
    with create_cli_cluster(id="add-user" + base_cluster_id, wait=True):
        cmd = base_cmd.copy()
        cmd.extend(["add-user", "--id", "sparktest"])
        process = execute_command(cmd)

        assert process.returncode != 0


def test_cluster_delete_success_basic():
    cmd = base_cmd.copy()
    cmd.append("delete")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_get_success_basic():
    cmd = base_cmd.copy()
    cmd.append("get")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_list_success_basic():
    cmd = base_cmd.copy()
    cmd.append("list")
    print(cmd)
    process = execute_command(cmd)
    print(process.stdout.read())
    print(process.stderr.read())
    assert process.returncode == 0


def test_cluster_app_logs_success_basic():
    cmd = base_cmd.copy()
    cmd.append("app-logs")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_ssh_success_basic():
    cmd = base_cmd.copy()
    cmd.append("ssh")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_submit_success_basic():
    cmd = base_cmd.copy()
    cmd.append("submit")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_run_success_basic():
    cmd = base_cmd.copy()
    cmd.append("run")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_copy_success_basic():
    cmd = base_cmd.copy()
    cmd.append("copy")
    process = execute_command(cmd)
    assert process.returncode != 0


def test_cluster_debug_success_basic():
    cmd = base_cmd.copy()
    cmd.append("debug")
    process = execute_command(cmd)
    assert process.returncode != 0
