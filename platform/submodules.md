# Information on how to pull from the top repostiory.

We are using [submodules](https://git.wiki.kernel.org/index.php/SubmoduleSupport) to populate this repository with the latest developments in project [emma](https://github.com/nlesc-sherlock/emma).

[Submodules tutorial](https://git.wiki.kernel.org/index.php/GitSubmoduleTutorial) shows how to create a superproject and then pull from there history and changes to a sub-directory into the local project. To merge local changes into the superproject repository it is recommended to use [git merge subtree](https://www.kernel.org/pub/software/scm/git/docs/howto/using-merge-subtree.html).

[Another tutorial](https://git-scm.com/book/en/v2/Git-Tools-Submodules) with all necessary steps to create a module, but also to manage it.

To update the module and merge changes do:
```
git submodule update --remote --merge
```

Make sure you have the ssh url and not the https url, otherwise, the 2 phase-login will give your troubles.
Then create a new branch and push it to the repository as we would do directly in a normal clone of the repository.

