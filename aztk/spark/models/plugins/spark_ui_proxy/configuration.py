import os
from aztk.models.plugins.plugin_configuration import PluginConfiguration, PluginPort, PluginRunTarget
from aztk.models.plugins.plugin_file import PluginFile
from aztk.utils import constants

dir_path = os.path.dirname(os.path.realpath(__file__))


class SparkUIProxyPlugin(PluginConfiguration):
    def __init__(self):
        super().__init__(
            name="spark_ui_proxy",
            ports=[
                PluginPort(
                    name="Spark UI",
                    internal=9999,
                    public=True
                )
            ],
            run_on=PluginRunTarget.Master,
            execute="spark_ui_proxy.sh",
            files=[
                PluginFile("spark_ui_proxy.sh", os.path.join(dir_path, "spark_ui_proxy.sh")),
                PluginFile("spark_ui_proxy.py", os.path.join(dir_path, "spark_ui_proxy.py")),
            ],
        )
