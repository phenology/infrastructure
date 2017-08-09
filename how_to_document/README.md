# How to document 
Here we describe best practices to document our project.<br>
Please see below our living collection of *"how to ...?"* questions, mostly linked to the use of git and github, where we use plain English to explain how to accomplish certain documentation related tasks.

## How to...

* How to start documenting a new project task (dataset, analysis)?
1. Go to the right github repository 
2. Do a `git pull` to make sure that you have the latest version of this repository
3. Do a `git status` to double check your current branch
4. if not in the *master* branch, go to it by using: `git checkout master`
5. Create a new (local) branch to start documenting a specific dataset and/or analysis: `git checkout -b NAMEOFBRANCH`   
6. Synchronize the local and github repositories by: `git push -u origin NAMEOFBRANCH`   
7. create a folder -ideally with the name of the branch- and a README.md file in it  

* How to create a new folder and a README.md file in github?  
1. Open github in your browser, go to the repository and branch where you want to work  
2. Click on *create new file*   
3. In the "prompt" type your folder name then */* and then README.md  
4. push the *commmit* buttom at the bottom of the page (eventually you can also add a commit message here)  

* How to create a new folder and a README.md file in bash+git?
1. `mkdir NAMEOFFOLDER`
2. `touch NAMEOFFOLDER/README.md`

* How to write markdown files? <br>
Markdown is a simple layout language to give format to text. 
Github uses a special form of markdown, simply called "Github-flavored markdown language" or GFM
For more info / get a quick reference, please check this [link](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)

* How to break the lines nicely?  
in GFM you can either use HTML `<br>` or you have to put *two* trailing spaces at the end of your sentences
(N.B. in the *source code* of this file you can find both examples)

* How to get the changes made in github (new files/documentation) to your local repository?  
Simply do a `git pull` 





