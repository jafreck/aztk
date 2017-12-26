# import asyncio
# import sys
# from concurrent.futures import ThreadPoolExecutor

# from azure.batch.models import batch_error

# import asyncssh
# from Crypto.PublicKey import RSA


# async def get_ssh_connection(node_ip, node_port, username, ssh_key=None, password=None):
#     conn = await asyncssh.connect(host=node_ip, port=node_port, client_keys=asyncssh.import_private_key(ssh_key), username=username, password=password, known_hosts=None)
#     return conn

# async def get_cluster_ssh_connection(nodes, username, ssh_key, password):
#     asyncio.get_event_loop().run_until_complete(
#         asyncio.wait(
#             [get_ssh_connection(node_ip=node.ip_address, node_port=node.port, username=username, ssh_key=ssh_key, password=password) for node in nodes]
#         )
#     )

# async def run_client(node_ip, node_port, ports, username, ssh_key=None, password=None):

#     async with asyncssh.connect(host=node_ip, port=node_port, client_keys=ssh_key, username=username, password=password) as conn:
#         listeners = []
#         for port in (ports or []):
#             listeners.append(await conn.forward_remote_port('', port, '', port))

#         async with conn.create_process('/bin/bash') as process:
#             while True:
#                 process.stdin.write(sys.stdin.readline())
#                 result = await process.stdout.readline()
#                 print(result, end='')


# async def execute_command(command, username, node_ip, node_port, ports=None, ssh_key=None, password=None):
#     # print(ssh_key)
#     # print(
#     #     [node_ip,
#     #      node_port,
#     #      [ssh_key.export_private_key('pkcs1-der')],
#     #      username,
#     #      password]
#     # )
#     import os
#     with open(os.path.expanduser("~/.ssh/test_key"), 'wb') as file:
#         file.write(ssh_key.export_private_key())
#     with open(os.path.expanduser("~/.ssh/test_key.pub"), 'wb') as file:
#         file.write(ssh_key.export_public_key())

#     async with asyncssh.connect(host=node_ip, port=node_port, client_keys=[ssh_key], username=username, password=password, known_hosts=None) as conn:
#         result = await conn.run(command=command)
#         if result.stdout:
#             print(result.stdout, end='')
#         else:
#             print(result.stderr, end='')


# async def cluster_run(command, username, nodes, ports=None, ssh_key=None, password=None):
#     await asyncio.wait(
#         [execute_command(command, username, node.ip_address, node.port, ports, ssh_key, password) for node in nodes]
#     )


# def node_run(command, username, node_ip, node_port, ports=None, ssh_key=None, password=None):
#     try:
#         asyncio.get_event_loop().run_until_complete(execute_command(command, username, node_ip, node_port, ports, ssh_key, password))
#     except (OSError, asyncssh.Error) as exc:
#         raise exc



# async def run_scp_client(node, source_path, destination_path, username, ssh_key = None, password = None, preserve=False, recurse=False):
#     conn = await get_ssh_connection(node.ip_address, node.port, username, ssh_key, password)
#     await asyncssh.scp(
#         source_path,
#         (conn, destination_path),
#         preserve=preserve,
#         recurse=recurse
#     )


# async def cluster_scp(username, nodes, source_path, destination_path, ssh_key=None, password=None, preserve=False, recurse=False):
#     await asyncio.gather(*[run_scp_client(node, source_path, destination_path, username, ssh_key, password, preserve, recurse) for node in nodes])









'''
    paramiko implementation
'''


import asyncio
import paramiko
import select
import socketserver as SocketServer
import io
import sys


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
        ssh_key.write_private_key_file('./tmp/id_rsa')
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
    client = await connect(hostname, port, username, password=password, pkey=ssh_key)
    stdin, stdout, stderr = client.exec_command(command, get_pty=True)
    [print(line.decode('utf-8')) for line in stdout.read().splitlines()]
    client.close()


async def clus_exec_command(command, username, nodes, ports=None, ssh_key=None, password=None):
    await asyncio.wait([node_exec_command(command, username, node.ip_address, node.port, ssh_key, password) for node in nodes])


async def node_copy(source_path, destination_path, username, hostname, port, ssh_key=None, password=None):
    client = await connect(hostname, port, username, password=password, pkey=ssh_key)
    sftp_client = client.open_sftp()
    sftp_client.put(source_path, destination_path)
    client.close()

    #TODO: progress bar

async def clus_copy(username, nodes, source_path, destination_path, ssh_key=None, password=None):
    await asyncio.wait([node_copy(source_path, destination_path, username, node.ip_address, node.port, ssh_key, password) for node in nodes])

async def shell(username, hostname, port, port_forward_list=[], password=None, ssh_key=None):
    client = await connect(hostname, port, username, password=password, pkey=ssh_key)

    for port_pair in port_forward_list:
        remote_port, local_port = port_pair
        try:
            forward_tunnel(local_port, hostname, remote_port, client.get_transport())
        except KeyboardInterrupt:
            print('C-c: Port forwarding stopped.')
            sys.exit(0)


class ForwardServer (SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

class Handler (SocketServer.BaseRequestHandler):

    def handle(self):
        try:
            chan = self.ssh_transport.open_channel('direct-tcpip',
                                                   (self.chain_host, self.chain_port),
                                                   self.request.getpeername())
        except Exception as e:
            verbose('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                              self.chain_port,
                                                              repr(e)))
            return
        if chan is None:
            verbose('Incoming request to %s:%d was rejected by the SSH server.' %
                    (self.chain_host, self.chain_port))
            return

        verbose('Connected!  Tunnel open %r -> %r -> %r' % (self.request.getpeername(),
                                                            chan.getpeername(), (self.chain_host, self.chain_port)))
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)

        peername = self.request.getpeername()
        chan.close()
        self.request.close()
        verbose('Tunnel closed from %r' % (peername,))


def forward_tunnel(local_port, remote_host, remote_port, transport):
    class SubHander (Handler):
        chain_host = remote_host
        chain_port = remote_port
        ssh_transport = transport
    ForwardServer(('', local_port), SubHander).serve_forever()

g_verbose = True

def verbose(s):
    if g_verbose:
        print(s)
