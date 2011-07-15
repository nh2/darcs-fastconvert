#!/bin/bash
. lib
set -ev

rm -rf R S T

darcs init --repo R
cd R
echo -e '1\n2\n3' > a
darcs add a
darcs rec -am aR
cd ..

darcs get R S

darcs get R T

cd T
darcs unpull -a
echo -e '1\n2\n3' > a
darcs add a
darcs rec -am aR
cd ..

darcs-fastconvert export R S
# Ensure we fail if initial patches aren't the same...
not darcs-fastconvert export R T
