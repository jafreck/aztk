import os
from aztk.models.plugins.plugin_configuration import PluginConfiguration, PluginPort, PluginTargetRole
from aztk.models.plugins.plugin_file import PluginFile
from aztk.utils import constants

dir_path = os.path.dirname(os.path.realpath(__file__))


def GATK4():
    return PluginConfiguration(
        name="gatk4",
        target_role=PluginTargetRole.Master,
        execute="gatk4.sh",
        files=[
            PluginFile("gatk4.sh", os.path.join(dir_path, "gatk4.sh")),
        ],
    )
