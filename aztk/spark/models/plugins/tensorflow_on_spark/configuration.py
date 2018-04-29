import os
from aztk.models.plugins.plugin_configuration import PluginConfiguration, PluginPort, PluginTargetRole
from aztk.models.plugins.plugin_file import PluginFile
from aztk.utils import constants

dir_path = os.path.dirname(os.path.realpath(__file__))


class TensorflowOnSparkPlugin(PluginConfiguration):
    def __init__(self):
        super().__init__(
            name="tensorflow_on_spark",
            target_role=PluginTargetRole.Master,
            execute="tensorflow_on_spark.sh",
            files=[
                PluginFile("tensorflow_on_spark.sh", os.path.join(dir_path, "tensorflow_on_spark.sh")),
            ],
        )
# NOTE: to run in distributed mode, HDFS must be present and added to LD_LIBRARY_PATH
