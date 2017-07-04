# Jupyter Notebooks

In this directory the user has examples of notebooks to read, process, and visualize data. To build his/her notebooks the reader should use the ones published under [examples](./examples). Under [examples](./examples) the notebooks are organized by kernel type.

## Write a new notebook
When developing a new NoteBook the user is advised to do control version of it, but neither use the [examples](./examples) or the [stable](./stable) directory. The latter is to publish stable notebooks for production and be used by other users. Furthermore, the directory structure at JupyterHub should be the same as the one used for control version.

## Upload them to the JupyterHub
The latest version of the Notebooks should always be the one uploaded to the cluster. It is advised to keep the repository version aligned as much as possible with the one uploaded at the cluster to avoid lost of work. Since it is a testing platform, there is high probability that the cluster will need to be reseted to default state, i.e., empty.

Having both versions synchornized also helps the user to have the latest version of the notebooks for local deployment, i.e., [in a cluster constructed using Vagrant boxes](https://github.com/nlesc-sherlock/emma/tree/d7e6016d3060173319e72173dc133b45ecfe4399#infra-structure) .


