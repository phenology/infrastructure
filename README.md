# Infrastructure
All work related with infrastructure. To clone it and also get the sub-module you should clone it in the following way:
```
#http version:
git clone --recursive https://github.com/phenology/infrastructure.git 

#ssh version:
git clone --recursive git@github.com:phenology/infrastructure.git
```

## Platform
Before developing or deploying an application it is necessary to get the platform up and running. Our platform uses [**Emma**](https://github.com/nlesc-sherlock/emma) project to setup a Spark Cluster with HDFS and Minio as storage layers. The user manages and interacts with the platform using a web-browser. The user should first follow the steps described in [platform](./platform) before developing or deploying an application. 


## Applications
Once the platform is up and running and data available the user should use examples under [applications/notebooks](./applications/notebooks) to create new interactive applications. For easy development we provide examples and we explain how to load the code and dependencies into [**IDEs**](./applications/ides).
