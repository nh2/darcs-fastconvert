#!/bin/bash
set -ev
. lib

rm -rf R git-R git_marks darcs_marks export_data

darcs init --repo R
git init git-R

cd R
echo 'a' > a && darcs add a && darcs rec -am 'Add a'

cd ..
darcs-fastconvert export R --write-marks=darcs_marks > export_data

(cd git-R && git fast-import --export-marks=../git_marks < ../export_data)

cd R
echo 'b' > b && darcs add b && darcs rec -am 'Add b'

cd ..
darcs-fastconvert export R --read-marks=darcs_marks > export_data

cd git-R
git fast-import --import-marks=../git_marks < ../export_data

git reset --hard HEAD
orig=$(git rev-parse HEAD)

[[ $(cat a) == 'a' && $(cat b) == 'b' ]]

git reset --hard HEAD~1

[[ $(cat a) == 'a' && ! -e b ]]

git reset --hard $orig

[[ $(git log --oneline | wc -l | awk '{print $1}') -eq 2 ]]
