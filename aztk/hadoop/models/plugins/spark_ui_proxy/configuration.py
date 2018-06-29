import os
from aztk.models.plugins.plugin_configuration import PluginConfiguration, PluginPort, PluginTargetRole
from aztk.models.plugins.plugin_file import PluginFile
from aztk.utils import constants

dir_path = os.path.dirname(os.path.realpath(__file__))


class hadoopUIProxyPlugin(PluginConfiguration):
    def __init__(self):
        super().__init__(
            name="hadoop_ui_proxy",
            ports=[
                PluginPort(
                    internal=9999,
                    public=True
                )
            ],
            target_role=PluginTargetRole.Master,
            execute="hadoop_ui_proxy.sh",
            args=["localhost:8080", "9999"],
            files=[
                PluginFile("hadoop_ui_proxy.sh", os.path.join(dir_path, "hadoop_ui_proxy.sh")),
                PluginFile("hadoop_ui_proxy.py", os.path.join(dir_path, "hadoop_ui_proxy.py")),
            ],
        )
