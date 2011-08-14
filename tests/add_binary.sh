#!/bin/bash
. lib
set -ev

rm -rf R-git

mkdir R-git
cd R-git
git init

touch a
git add a
git commit -m 'add a'

cp .git/index .
git add index
git commit -m 'Add index'

git fast-export HEAD | darcs-fastconvert import d

# ensure we recognise index as a binary file.
darcs cha --repo d -v | grep 'binary ./index'
