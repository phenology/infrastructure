# Platform

All the information in how to manage, up- and down- load data, and interact with platform is here summarized. The deployment of the platform only needs a list of machines and all the rest is taken cared by **Emma**.

Once the platform is up and running, the user only needs to upload the input data to the correct storage layer and then the platform is ready for [application developement](../applications).

## Emma
Emma is a project where ansible is used to setup a Spark cluster with GeoTrellis and SciSpark modules, and using for storage two flavors of storage, as file-based HDFS and GlusterFS and as object-based Minio (it has the same API as Amazon S3). 

**For this project the platform provision should only install a light version of the platform**. Such light platform does not have Docker-swarm and GlusterFS. To install such platform the user instead of running **ansible-playbook install_platform.yml**, as mentioned in [provision section](https://github.com/nlesc-sherlock/emma/blob/phenology/ansible.md#provision), the user should run the following:
```
ansible-playbook install_platform_light.yml --tags "common,minio,hadoop,spark,geotrellis,jupyterhub"
```
* To use an existing plattform, contact the owner listed below

Cloud provider | Cluster name | Owner/contact person
--- | --- | --- 
SURF-Sara HPC cloud | pheno | Raul/Romulo 

The owner will email a zip folder with all the configuration parameters. Unzip the folder and follow the instructions. Before that we recommend to [install ansible](https://github.com/nlesc-sherlock/emma/blob/master/ansible.md#install-ansible).

* To install the platform the user should read the instructions detailed in [**emma's** set up](https://github.com/nlesc-sherlock/emma/blob/master/README.md#setup-environment). This assumes that you use an Ubuntu machine or that you have Windows 10 with [**WSL**](https://msdn.microsoft.com/en-us/commandline/wsl/install_guide). If not, go to [**emma's** README](https://github.com/nlesc-sherlock/emma/blob/master/README.md).
* To update the **Hadoop** and **Spark** cluster of your platform please follow the instructions in [**emma's update existent platform**](https://github.com/nlesc-sherlock/emma/blob/master/ansible.md#update-an-existing-platform).
* To add new nodes to an existent cluster please read [**emma's add new node**](https://github.com/nlesc-sherlock/emma/blob/master/ansible.md#extend-an-existing-platform).

## Data loading
The platform provides two storage levels, a block-based storage through Hadoop Distributed FileSystem (HDFS) and a object-based sotrage through Minio.

### HDFS
HDFS is the distributed storage of Hadoop and to access it is required to install Hadoop binaries.

#### Hadoop binaries
Before The user needs to download the binaries for Hadoop 2.8.1 (it should be the same version as the one used in the cluster).
```
wget http://apache.hippo.nl/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz
tar -xzf hadoop-2.8.1.tar.gz
cd hadoop-2.8.1
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

#### Upload data
Now it is time to upload some data. The following example shows how to upload the GeoTiffs for the **spring-index**.
```
# Copy the files
./bin/hadoop dfs -copyFromLocal <path_to_spring-index>/spring-index/ /user/hadoop/

# In case you need to load into a specific user directory and you do not have write permissions
HADOOP_USER_NAME=pheno ./bin/hadoop dfs -copyFromLocal <path_to_data>/* /user/pheno/

# List the uploaded files
./bin/hadoop dfs -ls /user/hadoop/spring-index/
```

The uploaded files can also be listed using the [HDFS web-ui interface](https://github.com/nlesc-sherlock/emma/blob/223f93d91b63399cded51c52faa375ad77601fbd/hadoop.md#hadoop).

In case the user wants to reduce the size of the HDFS cluster, (s)he should remove two nodes at the time since the replication factor is *3* (HDFS default's replication factor). After each removal (s)he should rebalance the cluster. To do that login to one of the nodes, become root and run **hadoop balancer**:
```
cd infrastructure/platform/emma

# Login into pheno0
ssh -i files/pheno.key ubuntu@pheno0.phenovari-utwente.surf-hosted.nl

# Become root
sudo -i

# Run hadoop balancer
cd /usr/lib/hadoop
./bin/hadoop balancer -Ddfs.balancer.movedWinWidth=54000 -Ddfs.balancer.dispatcherThreads=10 -Ddfs.datanode.balance.max.concurrent.moves=10 -Ddfs.balance.bandwidthPerSec=100000000 -Ddfs.balancer.max-size-to-move=10737418200 -threshold 5sudo ./bin/hadoop balancer
```
If the user gets "*Error: JAVA_HOME is not set and could not be found.*", (s)he should [progate user's env variable to the root environment](https://unix.stackexchange.com/questions/6127/java-home-not-set-in-script-when-run-using-sudo).

The addition of nodes does not have any upper limit, however, it also requires a rebalance.

### Minio
[Minio](https://www.minio.io/) is a distributed object storage server built for cloud applications and devops.
To use minio in distributed mode and have redundancy there are some pre-requisites. To understand them you should read the [distributed minio quickstart guide](https://docs.minio.io/docs/distributed-minio-quickstart-guide). 

Minio web GUI is available though *http://<CLUSTER_NAME>0.phenovari-utwente.surf-hosted.nl:9091*, or any other host part of the *minio* group. For command line interaction we use [S3cmd tool for Amazon Simple Storage Service (S3)](https://github.com/s3tools/s3cmd). S3cmd is a free command line tool and client for uploading, retrieving and managing data in Amazon S3 and other cloud storage service providers that use the S3 protocol, such as Google Cloud storage and Minio. To install it in Ubuntu run the following command:
```
sudo apt-get install s3cmd
```

To access Minio in our platform the user should create **.s3cfg** at the his/her home directory and add the following information:
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

To upload data to a sub-directory the user needs to specify the sub-directories in the bucket's name. For example, to load all the files from **~/avhrr/SOST** into a bucket called **files/avhrr/SOST** the user should do the following:
```
cd ~/avhrr/SOST ; for f in `ls *`; do s3cmd put $f s3://files/avhrr/SOST/$f; done
```
#### Mount Minio bucket

It is possible to mount a minio bucket as a file system. For that we use [Goofys](https://github.com/kahing/goofys) which is a high-performance, POSIX-ish Amazon S3 file system written in Go. The user should follow its [installation](https://github.com/kahing/goofys#installation) and [usage](https://github.com/kahing/goofys#usage) guide to have a minio bucket mounted as a file system.

To mount buckets **files** in your local directory **~/minio** do:
```
mkdir ~/minio

#Replace <<cluster_name>> by your cluster name, such as emma, pheno, valencia etc.
./goofys --endpoint http://<<cluster_name>>0.phenovari-utwente.surf-hosted.nl:9091/ files ~/minio
```
