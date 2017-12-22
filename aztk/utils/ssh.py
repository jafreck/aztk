import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

from azure.batch.models import batch_error

import asyncssh
from Crypto.PublicKey import RSA


async def get_ssh_connection(node_ip, node_port, username, ssh_key=None, password=None):
    conn = await asyncssh.connect(host=node_ip, port=node_port, client_keys=asyncssh.import_private_key(ssh_key), username=username, password=password, known_hosts=None)
    return conn

async def get_cluster_ssh_connection(nodes, username, ssh_key, password):
    asyncio.get_event_loop().run_until_complete(
        asyncio.wait(
            [get_ssh_connection(node_ip=node.ip_address, node_port=node.port, username=username, ssh_key=ssh_key, password=password) for node in nodes]
        )
    )

async def run_client(node_ip, node_port, ports, username, ssh_key=None, password=None):

    async with asyncssh.connect(host=node_ip, port=node_port, client_keys=ssh_key, username=username, password=password) as conn:
        listeners = []
        for port in (ports or []):
            listeners.append(await conn.forward_remote_port('', port, '', port))

        async with conn.create_process('/bin/bash') as process:
            while True:
                process.stdin.write(sys.stdin.readline())
                result = await process.stdout.readline()
                print(result, end='')


async def execute_command(command, username, node_ip, node_port, ports=None, ssh_key=None, password=None):
    print(ssh_key)
    print(
        [node_ip,
         node_port,
         [ssh_key.export_private_key('pkcs1-der')],
         username,
         password]
    )
    import os
    with open(os.path.expanduser("~/.ssh/test_key"), 'wb') as file:
        file.write(ssh_key.export_private_key())
    with open(os.path.expanduser("~/.ssh/test_key.pub"), 'wb') as file:
        file.write(ssh_key.export_public_key())
    async with asyncssh.connect(host=node_ip, port=node_port, client_keys=[ssh_key], username=username, password=password, known_hosts=None) as conn:
        result = await conn.run(command=command)
        if result.stdout:
            print(result.stdout, end='')
        else:
            print(result.stderr, end='')


async def cluster_run(command, username, nodes, ports=None, ssh_key=None, password=None):
    await asyncio.wait(
        [execute_command(command, username, node.ip_address, node.port, ports, ssh_key, password) for node in nodes]
    )


def node_run(command, username, node_ip, node_port, ports=None, ssh_key=None, password=None):
    try:
        asyncio.get_event_loop().run_until_complete(execute_command(command, username, node_ip, node_port, ports, ssh_key, password))
    except (OSError, asyncssh.Error) as exc:
        raise exc



async def run_scp_client(node, source_path, destination_path, username, ssh_key = None, password = None, preserve=False, recurse=False):
    conn = await get_ssh_connection(node.ip_address, node.port, username, ssh_key, password)
    await asyncssh.scp(
        source_path,
        (conn, destination_path),
        preserve=preserve,
        recurse=recurse
    )


async def cluster_scp(username, nodes, source_path, destination_path, ssh_key=None, password=None, preserve=False, recurse=False):
    await asyncio.gather(*[run_scp_client(node, source_path, destination_path, username, ssh_key, password, preserve, recurse) for node in nodes])
