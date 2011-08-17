#!/bin/bash
. lib
set -ev

rm -rf R R_bridge

darcs init --repo R
cd R
echo testing > a && darcs add a && darcs rec -am 'Add a'
cd ..

darcs-fastconvert create-bridge R

cd R_bridge/R

cmds="help add remove move replace revert unrevert whatsnew record unrecord
amend-record mark-conflicts tag setpref diff changes annotate dist trackdown
show pull fetch obliterate rollback push send get put initialize optimize check
repair convert"

# ensure all commands but apply are disabled.
for cmd in `echo $cmds`
do
not darcs $cmd
done

# test that apply isn't disabled.
darcs apply --help | cat
