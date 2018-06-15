from aztk.core.models import Model, fields

class FileShare(Model):
    """
    Azure Files file share to mount to each node in the cluster

    Args:
        storage_account_name (int): the name of the Azure Storage Account
        storage_account_key (:obj:`str`, optional): the shared key to the Azure Storage Account
        file_share_path (:obj:`str`, optional): the path of the file share in Azure Files
        mount_path (:obj:`str`, optional): the path on the node to mount the file share 
    """
    storage_account_name = fields.String()
    storage_account_key = fields.String()
    file_share_path = fields.String()
    mount_path = fields.String()
