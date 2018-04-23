import os
from aztk.models.plugins.plugin_configuration import PluginConfiguration, PluginPort, PluginRunTarget
from aztk.models.plugins.plugin_file import PluginFile
from aztk.utils import constants

dir_path = os.path.dirname(os.path.realpath(__file__))


class TensorflowOnSpark(PluginConfiguration):
    def __init__(self):
        super().__init__(
            name="tensorflow_on_spark",
            run_on=PluginRunTarget.Master,
            execute="tensorflow_on_spark.sh",
            files=[
                PluginFile("spark_ui_proxy.sh", os.path.join(dir_path, "tensorflow_on_spark.sh")),
            ],
        )
# NOTE: to run in distributed mode, HDFS must be present and added to LD_LIBRARY_PATH
