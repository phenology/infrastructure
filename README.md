# Infrastructure
All work related with infrastructure.   

* Step 1
  * If you haven't clone the repository, then clone it as follows:
    The recursive keyword is needed because it will also clone a submodule (in this case called **emma**)
  ```
  #http version:
  git clone --recursive https://github.com/phenology/infrastructure.git 

  #ssh version:
  git clone --recursive git@github.com:phenology/infrastructure.git
  ```
  * If you have cloned the repository but without the submodule, then do as follows:
  ```
  cd infrastructure 
  git submodule init
  git submodule update
  ```
* Step 2
  Enter the submodule and go to the *phenology* branch:
  ```
  cd infrastructure/<submodule_name>
  git checkout phenology
  ```
  
## Platform
Before developing or deploying an application it is necessary to get the platform up and running. Our platform uses [**Emma**](https://github.com/nlesc-sherlock/emma) project to setup a Spark Cluster with HDFS and Minio as storage layers. The user manages and interacts with the platform using a web-browser. The user should first follow the steps described in [platform](./platform) before developing or deploying an application. 


## Applications
Once the platform is up and running, and data available, the user can start developing and/or deploying [applications](applications). For development of a new application the user should use examples under [applications/notebooks](./applications/notebooks). For easy development there are examples and information in how to load the code and dependencies into [**IDEs**](./applications/ides).
