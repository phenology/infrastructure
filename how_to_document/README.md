# How to ...  

Here we describe best practices to document our project.  
Please see below our collection of *"how to ...?"* questions, mostly linked to the use of git and github, 
where we use plain English to explain how to accomplish certain documentation related tasks.

Table of Contents
=================
* [How to start documenting a new project task (dataset, analysis)?](#how-to-start-documenting-a-new-project-task-dataset-analysis)
* [How to write markdown files? <br>](#how-to-write-markdown-files-)
* [How to break lines in markdown files?](#how-to-break-lines-in-markdown-files)
* [How to get the changes made in github (new files/documentation) to your local repository?](#how-to-get-the-changes-made-in-github-new-filesdocumentation-to-your-local-repository)
* [How to rename a folder or a file in a repository?](#how-to-rename-a-folder-or-a-file-in-a-repository)
* [How to add a new file to the repository?](#how-to-add-a-new-file-to-the-repository)
* [How to merge a branch into the master branch?](#how-to-merge-a-branch-into-the-master-branch)
* [How to create a table of contents?](#how-to-create-a-table-of-contents)

---------------

## How to start documenting a new project task (dataset, analysis)?

1. Go to the right github repository 
3. Do a `git status` to double check your current branch
4. if not in the *master* branch, go to it by using: `git checkout master`
2. Do a `git pull` to make sure that you have the latest version of this repository
5. Create a new (local) branch to start documenting a specific dataset and/or analysis: `git checkout -b NAMEOFBRANCH`   
6. Synchronize the local and github repositories by: `git push -u origin NAMEOFBRANCH`   
7. Create a folder -ideally with the name of the branch- and a README.md file in it  
  * in github  
     1. Open github in your browser, go to the repository and branch where you want to work  
     2. Click on *create new file*   
     3. In the "prompt" type your folder name then */* and then type a FILENAME, for example README.md  
     4. push the *commmit* buttom at the bottom of the page and do not forget to use an informative (i.e. what and why) commit message  

  * in bash+git
    1. `mkdir NAMEOFFOLDER`
    2. `touch NAMEOFFOLDER/README.md` (NB: For more info about *touch* see [this](http://www.linfo.org/touch.html))

8. Synchronize by
    1. `git add NAMEOFFOLDER`
    2. `git commit NAMEOFFOLDER -m "beginning of documentation"`

## How to write markdown files? <br>
Markdown is a simple layout language to give format to text. 
Github uses a special form of markdown, simply called "Github-flavored markdown language" or GFM
For more info / get a quick reference, please check this [link](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)

## How to break lines in markdown files?  
In GFM you can either use HTML `<br>` or you have to put *two* trailing spaces at the end of your sentences
(N.B. in the *source code* of this file you can find both examples)

## How to get the changes made in github (new files/documentation) to your local repository?  
Simply do a `git pull` 

## How to rename a folder or a file in a repository?
It is good practice to always use smallcase (file and folder names)
1. Go out of the folder with capital letters (i.e. go one level up by simply typing `cd ..`)
2. `git mv FOLDERNAME foldername`
3. `git commit foldername -m "rename folder or file"`
4. `git push`

## How to add a new file to the repository?
1. copy local file to the desired location in the local repository
2. map to the location (i.e. to the folder where the file was just copied)
3. `git add FILENAME` use tab to autocomplete. Git knows that this file is not being tracked 
4. `git commit FILENAME -m "MSG" `
5. `git push`

## How to merge a branch into the master branch?
  * As owner of the branch:
    1. Use `git merge master` to make sure that the branch is aligned with the contents of the master branch
    1. Create a pull request using github with a summary of what the branch does
    2. Assign a reviewer 
    3. if any, solve the comments of the reviewer (ideally in a single commit per comment)
    4. once the branch is merged into the master:
      - `git checkout master` 
      - `git pull`
      - `git branch -d BRANCHNAME` (to remove/delete the branch)
      
  * As reviewer:
    1. check the branch and give commnents (per commit or for all the branch)
    2. any change to be done to the file(s), should be requested in those comments
    3. once all the comments have been fixed by the owner:
      - add a final comment indicating tha the branch is ready to be merged
      - merge branch into the master and confirm the merge (using github buttons)
      - delete branch (using github button)  
	  
 ## How to create a table of contents?
 Here we describe the lazy way of doing this (and this is how the TOC of this README file was created).  
 Go to this [link](https://github.com/ekalinin/github-markdown-toc), and follow the installation instructions to get *gh-md-toc*.  
 In your bash console (this only works in Ubuntu/Linux systems), type the following (adjust your path as needed):
  
 `./gh-md-toc ../infrastructure/how_to_document/README.md` 
 
 Then copy/paste the result from console into your original README.md file.
 NB: do not forget to commit and push the file to the repository
 NB: if you need to create the TOC again, it is better to remove the "old TOC" from the file before calling *gh-md-toc* again ;-)
 
