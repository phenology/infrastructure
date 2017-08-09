# Infrastructure
All work related with infrastructure. To clone it and also get the sub-module you should clone it in the following way:
```
#http version:
git clone --recursive https://github.com/phenology/infrastructure.git 

#ssh version:
git clone --recursive git@github.com:phenology/infrastructure.git
```

## EMMA
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

## Applications

To develop applications and deploy them on the installed platform, the user should first follow the steps described in [applications](./applications) to load data. Once the data is loaded the user has two ways to develop an application, through [interactive notebooks](./applications/notebooks) or offline jobs using. For easy development we provide examples and we explain how to load the code and dependencies into [**IDEs**](./applications/ides).
