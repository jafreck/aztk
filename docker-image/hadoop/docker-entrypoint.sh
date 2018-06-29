#!/bin/bash
set -e

echo "- Starting all HDFS processes under supervisord"
/usr/bin/supervisord --configuration /etc/supervisord.conf

# MASTER_IP=$(echo -e $(hostname -I) | tr -d '[:space]')

# Create a directory for the hadoop file system
mkdir -p /mnt/batch/hadoop
echo '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hdfs-namenode:8020</value>
    </property>
</configuration>' > /etc/hadoop/core-site.xml

echo '<?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
        <property>
            <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
            <value>false</value>
        </property>
        <property>
            <name>dfs.datanode.data.dir</name>
            <value>file://mnt/batch/hadoop</value>
        </property>
    </configuration>' > /etc/hadoop/hdfs-site.xml


echo $1

if [ $1 == "hdfs-namenode" ]; then
    supervisorctl start hdfs-namenode
fi
if [ $1 == "hdfs-datanode" ]; then
    supervisorctl start hdfs-datanode
fi
if [ $1 == "yarn-resourcemanager" ]; then
    supervisorctl start yarn-resourcemanager
fi
if [ $1 == "yarn-nodemanager" ]; then
    echo $2
    echo "<?xml version='1.0' encoding='UTF-8'?>
        <?xml-stylesheet type='text/xsl' href='configuration.xsl'?>
        <configuration>
            <property>
                <name>yarn.resourcemanager.hostname</name>
                <value>$2</value>
            </property>
        </configuration>" > /etc/hadoop/yarn-site.xml
    supervisorctl start yarn-nodemanager
fi
if [ $1 == "yarn-proxyserver" ]; then
    supervisorctl start yarn-proxyserver
fi


tail -F /dev/null