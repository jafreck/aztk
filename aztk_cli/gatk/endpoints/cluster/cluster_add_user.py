import argparse
import typing

import aztk.gatk
from aztk_cli import config, log, utils


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id', dest='cluster_id', required=True,
                        help='The unique id of your gatk cluster')
    parser.add_argument('-u', '--username',
                        help='The usernameto access your gatk cluster\'s head node')

    auth_group = parser.add_mutually_exclusive_group()
    auth_group.add_argument('-p', '--password',
                            help="The password to access your gatk cluster's master node. If not provided will use ssh public key.")
    auth_group.add_argument('--ssh-key',
                            help="The ssh public key to access your gatk cluster's master node. You can also set the ssh-key in the configuration file.")
    parser.set_defaults(username="admin")


def execute(args: typing.NamedTuple):
    gatk_client = aztk.gatk.Client(config.load_aztk_secrets())

    log.info('-------------------------------------------')
    log.info('gatk cluster id:    {}'.format(args.cluster_id))
    log.info('username:            {}'.format(args.username))
    log.info('-------------------------------------------')

    if args.ssh_key:
        ssh_key = args.ssh_key
    else:
        ssh_key = gatk_client.secrets_config.ssh_pub_key

    ssh_key, password = utils.get_ssh_key_or_prompt(ssh_key, args.username, args.password, gatk_client.secrets_config)

    gatk_client.create_user(
        cluster_id=args.cluster_id,
        username=args.username,
        password=password,
        ssh_key=ssh_key
    )

    if password:
        log.info('password:            %s', '*' * len(password))
    elif ssh_key:
        log.info('ssh public key:      %s', ssh_key)

    log.info('-------------------------------------------')
