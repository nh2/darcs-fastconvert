#!/bin/bash
set -ev
rm -rf R S R2 R2-head-S

darcs init --repo R
cd R
touch a && darcs add a && darcs rec -am a
touch b && darcs add b && darcs rec -am b
touch d && darcs add d && darcs rec -am d
cd ..

darcs get R S
cd S
echo 'yd' | darcs unpull
touch c && darcs add c && darcs rec -am c
cd ..

darcs-fastconvert export R S | darcs-fastconvert import R2

[[ $(darcs cha --count --repo R2) -eq 3 &&
   $(darcs cha --count --repo R2-head-S) -eq 3 ]]

[[ -e R2/a && -e R2/b && -e R2/d && (! -e R2/c) &&
   -e R2-head-S/a && -e R2-head-S/b && -e R2-head-S/c && (! -e R2-head-S/d) ]]
