# Clustering CONUS and Europe Spring-Indices with CGC

## Infrastructure

All datasets have been run on the SURF Sara HPC Cloud, using computing time that has
been granted to the project [High spatial resolution phenological modelling at continental scales](https://www.esciencecenter.nl/projects/high-spatial-resolution-phenological-modelling-at-continental-scales/).
The 'parrot' cluster, consisting of four virtual machines (VMs), has been employed. The VMs can be 
deployed from the [HPC Cloud access portal](http://ui.hpccloud.surfsara.nl): navigate to the 
`Instances` > `VMs` menu on the left side, select the VMs whose `Name` field starts with 'parrot'
and press the 'play' button. The VMs are up and running when the `Status` field is labeled as `RUNNING`.
When the VMs are not needed anymore, they can be undeployed in order to release the allocated resources:
select the VMs, then from the menu that appears when clicking on the 'power' button select `Undeploy`. 
The VMs are successfully undeployed when the `Status` field is labeled as `UNDEPLOYED`.

Software and services on the 'parrot' cluster have been managed using 
[Emma](https://github.com/nlesc-sherlock/emma), as described in the 
[infrastructure](https://github.com/phenology/infrastructure) repository. 
Clone the latter and follow the instructions in the README.md file 
to retrieve Emma and to checkout its `phenology` branch. 

To get started with Emma create a `env_linux.sh` file by copying the 
corresponding template (`env_linux.sh.template`), then edit the fields 
`CLUSTER_NAME`, `HOST_DOMAIN` and `NUM_HOSTS`. Make sure the 'parrot' 
private SSH key is present in the same folder and named `parrot.key`, 
then run
```shell
cd <path_to_emma>/emma
. env_linux.sh
./create_hosts.sh 
cd vars/
./create_vars_files.sh

```

Emma requires [Ansible](https://docs.ansible.com). A python 
environment with Ansible v2.3 installed (as recommended for 
Emma) can be created using 
`conda`
```shell
conda create -n emma ansible=2.3 -c conda-forge 
```
and activated as
```shell
conda activate emma
```

Emma's [documentation](https://github.com/nlesc-sherlock/emma/blob/master/ansible.md) 
illustrates how to install a python package on all of the cluster's VMs 
and how to start one of the supported services. Briefly, to 
install a package  via `pip`, either from PyPI
or from a GitHub repository, add an element to the
`python_packages` list in the file `vars/common_vars.yml`.

For instance, to install (or update) [CGC](https://github.com/phenology/cgc) 
modify `vars/common_vars.yml` so that it includes
```yaml
python_packages:
    - clustering-geodata-cubes
```
to get CGC from the corresponding [PyPI project](https://pypi.python.org/project/clustering-geodata-cubes/), or
```yaml
python_packages:
    - https://github.com/phenology/cgc/archive/development.zip
```
to clone and install a specific branch (in this case, the 
`development` branch) from the CGC [GitHub repository](https://github.com/phenology/cgc).
The package is then installed on all VMs via:
```shell
ansible-playbook install_platform_light.yml --tags "extra_python_packages" --private-key=parrot.key
```

Relevant services for this project are 
JupyterHub and Dask. The JupyterHub server runs on `parrot0`,
it can be started as
```shell
ansible-playbook start_platform.yml --tags "jupyterhub" --private-key=parrot.key
```
In order to connect to JupyterHub's login page,
forward port `8000` from `parrot0`
to your local machine within a SSH tunnel
```shell
ssh -N -L 8000:localhost:8000 -i parrot.key ubuntu@parrot0.${HOST_DOMAIN} 
```
then open a browser window and connect to `localhost:8000`.
Terminate the SSH tunnel after logging out.
To shut down the JupyterHub server:
```shell
ansible-playbook shutdown_platform.yml --tags "jupyterhub" --private-key=parrot.key
```

The Dask (SSH) cluster can be started as
```shell
ansible-playbook start_platform.yml --tags "dask" --private-key=parrot.key
```
A worker with 8 threads is started on each of the
four VMs of the cluster, with the scheduler running 
on `parrot0`. To connect to the Dask cluster from a 
Python script/interactive Python session/Jupyter notebook
running on any of the parrot's nodes, type:
```python
from dask.distributed import Client
client = Client('parrot0:9091')
```
The Dask dashboard can be accessed from your browser
at `localhost:8787` after forwarding the corresponding port:
```shell
ssh -N -L 8787:localhost:8787 -i parrot.key ubuntu@parrot0.${HOST_DOMAIN} 
```
In order to shut down the Dask cluster:
```shell
ansible-playbook shutdown_platform.yml --tags "dask" --private-key=parrot.key
```
