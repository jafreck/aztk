import argparse
import os
import sys
import typing

import aztk.gatk
from aztk_cli import config, log, utils
from aztk_cli.config import SshConfig


def setup_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--id', dest='cluster_id', required=True,
                        help='The unique id of your gatk cluster')

    parser.add_argument('--name', required=True,
                        help='a name for your application')
    parser.add_argument('-u', '--username', help='Username to gatk cluster')

    parser.add_argument('--wait', dest='wait', action='store_true',
                        help='Wait for app to complete')
    parser.add_argument('--no-wait', dest='wait', action='store_false',
                        help='Do not wait for app to complete')
    parser.set_defaults(wait=True)

    parser.add_argument('--output',
                        help='Path to the file you wish to output to. If not \
                              specified, output is printed to stdout')

    parser.add_argument('command', nargs='*',
                        help='The gatk command to run')


def execute(args: typing.NamedTuple):
    if not args.wait and args.output:
        raise aztk.error.AztkError("--output flag requires --wait flag")

    gatk_client = aztk.gatk.Client(config.load_aztk_secrets())
    # jars = []
    # py_files = []
    # files = []

    # if args.jars is not None:
    #     jars = args.jars.replace(' ', '').split(',')

    # if args.py_files is not None:
    #     py_files = args.py_files.replace(' ', '').split(',')

    # if args.files is not None:
    #     files = args.files.replace(' ', '').split(',')

    # log.info("-------------------------------------------")
    # log.info("gatk cluster id:        %s", args.cluster_id)
    # log.info("gatk app name:          %s", args.name)
    # log.info("Wait for app completion: %s", args.wait)
    # if args.main_class is not None:
    #     log.info("Entry point class:       %s", args.main_class)
    # if jars:
    #     log.info("JARS:                    %s", jars)
    # if py_files:
    #     log.info("PY_Files:                %s", py_files)
    # if files:
    #     log.info("Files:                   %s", files)
    # if args.driver_java_options is not None:
    #     log.info("Driver java options:     %s", args.driver_java_options)
    # if args.driver_library_path is not None:
    #     log.info("Driver library path:     %s", args.driver_library_path)
    # if args.driver_class_path is not None:
    #     log.info("Driver class path:       %s", args.driver_class_path)
    # if args.driver_memory is not None:
    #     log.info("Driver memory:           %s", args.driver_memory)
    # if args.executor_memory is not None:
    #     log.info("Executor memory:         %s", args.executor_memory)
    # if args.driver_cores is not None:
    #     log.info("Driver cores:            %s", args.driver_cores)
    # if args.executor_cores is not None:
    #     log.info("Executor cores:          %s", args.executor_cores)
    # log.info("Application:             %s", args.app)
    # log.info("Application arguments:   %s", args.app_args)
    # log.info("-------------------------------------------")


    # gatk_client.submit(
    #     cluster_id=args.cluster_id,
    #     application = aztk.gatk.models.ApplicationConfiguration(
    #         name=args.name,
    #         application=args.app,
    #         application_args=args.app_args,
    #         main_class=args.main_class,
    #         jars=jars,
    #         py_files=py_files,
    #         files=files,
    #         driver_java_options=args.driver_java_options,
    #         driver_library_path=args.driver_library_path,
    #         driver_class_path=args.driver_class_path,
    #         driver_memory=args.driver_memory,
    #         executor_memory=args.executor_memory,
    #         driver_cores=args.driver_cores,
    #         executor_cores=args.executor_cores,
    #         max_retry_count=args.max_retry_count
    #     ),
    #     wait=False
    # )

    # if args.wait:
    #     if not args.output:
    #         exit_code = utils.stream_logs(client=gatk_client, cluster_id=args.cluster_id, application_name=args.name)
    #     else:
    #         with utils.Spinner():
    #             gatk_client.wait_until_application_done(cluster_id=args.cluster_id, task_id=args.name)
    #             application_log = gatk_client.get_application_log(cluster_id=args.cluster_id, application_name=args.name)
    #             with open(os.path.abspath(os.path.expanduser(args.output)), "w", encoding="UTF-8") as f:
    #                 f.write(application_log.log)
    #             exit_code = application_log.exit_code

    #     sys.exit(exit_code)

    from aztk.utils.ssh import node_exec_command


    cluster = gatk_client.get_cluster(args.cluster_id)
    configuration = gatk_client.get_cluster_config(args.cluster_id)

    master_node_id = cluster.master_node_id

    ssh_conf = SshConfig()

    ssh_conf.merge(
        cluster_id=args.cluster_id,
        username=args.username,
        job_ui_port=None,
        job_history_ui_port=None,
        web_ui_port=None,
        host=None,
        connect=None,
        internal=None)

    node_id, output = gatk_client.node_run(
        cluster_id=args.cluster_id,
        node_id=master_node_id,
        command="docker exec gatk /bin/bash -c 'source /root/.gatkbashrc; echo $PATH; gatk" + args.command,
    )

    print(output)
