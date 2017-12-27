'''
    SSH utils
'''


import asyncio
import io
import select
import socketserver as SocketServer
import sys

import paramiko


async def connect(hostname,
                  port=22,
                  username=None,
                  password=None,
                  pkey=None,
                  key_filename=None,
                  timeout=None,
                  allow_agent=True,
                  look_for_keys=True,
                  compress=False,
                  sock=None,
                  gss_auth=False,
                  gss_kex=False,
                  gss_deleg_creds=True,
                  gss_host=None,
                  banner_timeout=None,
                  auth_timeout=None,
                  gss_trust_dns=True,
                  passphrase=None):

    client = paramiko.SSHClient()

    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if pkey:
        ssh_key = paramiko.RSAKey.from_private_key(file_obj=io.StringIO(pkey))
    else:
        ssh_key = None

    client.connect(
        hostname,
        port=port,
        username=username,
        password=password,
        pkey=ssh_key,
        key_filename=key_filename,
        timeout=timeout,
        allow_agent=allow_agent,
        look_for_keys=look_for_keys,
        compress=compress,
        sock=sock,
        gss_auth=gss_auth,
        gss_kex=gss_kex,
        gss_deleg_creds=gss_deleg_creds,
        gss_host=gss_host,
        banner_timeout=banner_timeout,
        auth_timeout=auth_timeout,
        gss_trust_dns=gss_trust_dns,
        passphrase=passphrase
    )

    return client


async def node_exec_command(command, username, hostname, port, ssh_key=None, password=None):
    client = await connect(hostname=hostname, port=port, username=username, password=password, pkey=ssh_key)
    stdin, stdout, stderr = client.exec_command(command, get_pty=True)
    [print(line.decode('utf-8')) for line in stdout.read().splitlines()]
    client.close()


async def clus_exec_command(command, username, nodes, ports=None, ssh_key=None, password=None):
    await asyncio.wait([node_exec_command(command, username, node.ip_address, node.port, ssh_key, password) for node in nodes])


async def node_copy(source_path, destination_path, username, hostname, port, ssh_key=None, password=None):
    client = await connect(hostname=hostname, port=port, username=username, password=password, pkey=ssh_key)
    sftp_client = client.open_sftp()
    sftp_client.put(source_path, destination_path)
    client.close()

    #TODO: progress bar

async def clus_copy(username, nodes, source_path, destination_path, ssh_key=None, password=None):
    await asyncio.wait(
        [node_copy(source_path=source_path,
                   destination_path=destination_path,
                   username=username,
                   hostname=node.ip_address,
                   port=node.port,
                   ssh_key=ssh_key,
                   password=password) for node in nodes
        ])
