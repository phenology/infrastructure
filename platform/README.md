# Platform

All the information in how to manage, up- and down- load data, and interact with platform is here summarized.

### Emma
Emma is a project where ansible is used to setup a Spark cluster with GeoTrellis and SciSpark modules, and using for storage two flavors of storage, as file-based HDFS and GlusterFS and as object-based Minio (it has the same API as Amazon S3). To install the platform the user should read the instructions detailed in [**emma's** README](https://github.com/nlesc-sherlock/emma/blob/master/README.md).

**For this project the platform provision should only install a light version of the platform**. Such light platform does not have Docker-swarm and GlusterFS. To install such platform the user instead of running **ansible-playbook install_platform.yml**, as mentioned in [provision section](https://github.com/nlesc-sherlock/emma/blob/documentation/ansible.md#provision), the user should run the following:
```
ansible-playbook playbooks/install_spark.yml
```

The platform only needs to be installed once. Once it is installed the services, e.g., Hadoop and Spark, are started using the following command:
```
ansible-playbook start_platform.yml
```

To shutdown the platform just run the following command:
```
ansible-playbook shutdown_platform.yml
```
## Data loading

### HDFS

Before The user needs to download the binaries for Hadoop 2.8.0.
```
wget http://apache.hippo.nl/hadoop/common/hadoop-2.8.0/hadoop-2.8.0.tar.gz
tar -xzf hadoop-2.8.0.tar.gz
cd hadoop-2.8.0
```
Copy the Hadoop configuration environment, **core-site.xml** and **hdfs-site.xml**, from one of the virtual machines. Its location at the remote machine is **/etc/hadoop/conf/** and its destination is **etc/hadoop/**.
Once the hadoop configuration is copied it is time to test it and for that let's list the user directories.
```
./bin/hadoop dfs -ls /user

#The outcome should be the following:
Found 4 items
drwxr-xr-x   - hadoop hadoop              0 2017-06-08 10:03 /user/hadoop
drwxrwxrwt   - root   supergroup          0 2017-06-08 10:03 /user/history
drwxr-xr-x   - spark  spark               0 2017-06-08 10:04 /user/spark
drwxr-xr-x   - ubuntu ubuntu              0 2017-06-08 10:04 /user/ubuntu
```

Not it is time to upload some data. The following example shows how to upload the GeoTiffs for the **spring-index**.
```
# Copy the files
./bin/hadoop dfs -copyFromLocal <path_to_spring-index>/spring-index/ /user/hadoop/

# List the uploaded files
./bin/hadoop dfs -ls /user/hadoop/spring-index/
```

The uploaded files can also be listed using the [HDFS web-ui interface](https://github.com/nlesc-sherlock/emma/blob/223f93d91b63399cded51c52faa375ad77601fbd/hadoop.md#hadoop).

### Minio
[Minio](https://www.minio.io/) is a distributed object storage server built for cloud applications and devops.
To use minio in distributed mode and have redundancy there are some pre-requisites. To understand them you should read the [distributed minio quickstart guide](https://docs.minio.io/docs/distributed-minio-quickstart-guide). 

Minio web GUI is available though *http://pheno0.phenovari-utwente.surf-hosted.nl:9091*, or any other host part of the *minio* group.
For command line interaction we use [S3cmd tool for Amazon Simple Storage Service (S3)](https://github.com/s3tools/s3cmd). S3cmd is a free command line tool and client for uploading, retrieving and managing data in Amazon S3 and other cloud storage service providers that use the S3 protocol, such as Google Cloud storage and Minio.

To access Minio in our platform the user should create **.s3cfg** at the his/her home directory.
```
host_base = <IP_pheno0>:9091
host_bucket = <IP_pheno0>:9091
access_key = <access_key>
secret_key = <secret_key>
use_https = False
list_md5 = False
use_mime_magic = False
#Make sure the region is the same as the one used by minio
bucket_location = us-east-1
```

Example of commands:
```
s3cmd  ls s3://files
s3cmd get s3://files/sonnets.txt sonnets.txt
```

To upload data to a sub-directory follow this example:
```
cd <root_dir>/<sub_dir> ; for f in `ls *`; do s3cmd put $f s3://<root_dir>/<sub_dir>/$f; done
```