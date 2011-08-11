#!/bin/bash
. lib
set -ev

rm -rf gitfoo* darcsfoo*

git init gitfoo
cd gitfoo

echo 'a' > a && git add a && git commit -m 'Add a'

git checkout -b branch1

echo 'branch1' > b && git add b && git commit -m 'Add b branch1'

git checkout master

echo 'master' > c && git add c && git commit -m 'Add c master'

git merge branch1 --no-ff


git fast-export --all | darcs-fastconvert import ../darcsfoo

cd ..

git init gitfoo2
darcs-fastconvert export darcsfoo* | (cd gitfoo2 && git fast-import)

cd gitfoo2

git reset --hard HEAD

read a_content < a
read b_content < b
read c_content < c

# Let's ensure both branches are re-exported and all files are present, with
# the correct contents.
[[ $(git branch -a | wc -l | awk '{print $1}') -eq 2 && -e a && -e b && -e c
   && $a_content == 'a' && $b_content == 'branch1' && $c_content == 'master' ]]

