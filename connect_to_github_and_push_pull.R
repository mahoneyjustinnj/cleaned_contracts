setwd("/cloud/project/cleaning_script/read_SQL/clean_contracts_efficient_250820/cleaned_contracts")
# this system command is same as running from the command line
# gives list of remote repositories connected to your local Git project,
system("git remote -v")
# result:
# origin	https://github.com/mahoneyjustinnj/cleaned_contracts (fetch)
# origin	https://github.com/mahoneyjustinnj/cleaned_contracts (push)
#means
# origin is the name of the remote (default name for the main GitHub link).
# the URL is the location of your GitHub repo.
# (fetch) means it pulls updates from GitHub.
# (push) means it sends your changes to GitHub.

#check that the github repo is the 'master' (or main) branch
system("git branch")
# result:
#   * master

# This will fetch and merge any new commits from your GitHub repo (cleaned_contracts) into your Posit Cloud workspace.
system("git pull origin master")
#result was to pll in the following 3 files (below)
# Fast-forward
# database.sqlite3           | Bin 0 -> 114688 bytes
# first_read_in_data250821.R | 104 +++++++++++++++
#   grc_service_contracts.csv  | 310 +++++++++++++++++++++++++++++++++++++++++++++
#   3 files changed, 414 insertions(+)
# create mode 100644 database.sqlite3
# create mode 100755 first_read_in_data250821.R
# create mode 100755 grc_service_contracts.csv

# the following gives the log of that just happened
system("git log --oneline")

#this gives the status of the pushes and pulls
system("git status")
#result:
# On branch master
# Your branch is up to date with 'origin/master'.
# Untracked files:
#   (use "git add <file>..." to include in what will be committed)
# .gitignore
# connect_to_github_and_push_pull.R
# nothing added to commit but untracked files present (use "git add" to track)

# This command stages the .gitignore file and the connect_to_github_and_push_pull.R script,
# preparing them to be included in the next commit. It tells Git to start tracking these files.
system("git add .gitignore connect_to_github_and_push_pull.R")

# This command creates a new commit with the staged files and attaches a message describing the changes.
# In this case, it records the addition of the .gitignore file and the GitHub connection script.
system("git commit -m 'Add .gitignore and GitHub connection script'")
#result
# [master 655bba8] Add .gitignore and GitHub connection script
# 2 files changed, 51 insertions(+)
# create mode 100644 .gitignore
# create mode 100644 connect_to_github_and_push_pull.R

# This command pushes the latest commit(s) from your local 'master' branch to the remote GitHub repository.
# It updates the GitHub repo with any new changes you've committed in Posit Cloud.
system("git push origin master")
#result
# To https://github.com/mahoneyjustinnj/cleaned_contracts
# 3fefe7d..655bba8  master -> master

#########second addition to github
#first, i want to add the untracted files to github 
#all at once using:
system("git add .")
#or, 1 at a time using
# git add check_sqlite_data_250903.R
# git add clean_grc_data.R
# git add eco_obj_code_TO_USE.csv
# git add econ_obj_code.csv
# git add grc_cleaned_final_final_250904.csv

#2nd - i will commit the changes using:
system("git commit -m 'Add new data and scripts'  ")
# 3rd - i will push the changes to github
system("git push origin master")
#result
# To https://github.com/mahoneyjustinnj/cleaned_contracts
# 655bba8..720e306  master -> master
#get the status
system("git status")
# On branch master
# Your branch is up to date with 'origin/master'.






