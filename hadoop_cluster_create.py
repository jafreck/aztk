import sys

from aztk.hadoop import Client
from aztk.hadoop import models 
from aztk_cli import config
from aztk.models import Toolkit

secrets_configuration = config.load_aztk_secrets()
hadoop_client = Client(secrets_configuration)


cluster_configuration = models.ClusterConfiguration(
    cluster_id="hadoop-test-" + sys.argv[1],
    toolkit=Toolkit(software="hadoop", version="3.1.0", docker_repo="aztk/staging:hadoop3.1.0"),
    vm_size="standard_d2",
    size=10
)


cluster = hadoop_client.create_cluster(cluster_configuration, wait=True)
# cluster = hadoop_client.get_cluster(cluster_configuration.cluster_id)

hadoop_client.create_user(cluster.id, username="hadoop", ssh_key=secrets_configuration.ssh_pub_key)

port_forward_list = [models.PortForwardingSpecification(remote_port=8088, local_port=8088)]
hadoop_client.cluster_ssh_into_master(cluster.id, cluster.master_node_id, port_forward_list=port_forward_list, username="hadoop")
