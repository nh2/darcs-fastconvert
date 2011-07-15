#!/bin/bash
. lib
rm -rf R R_bridge S

darcs init --repo R
cd R
touch a && darcs add a && darcs rec -am 'add a'
cd ..

darcs-fastconvert create-bridge R

bridge_path=R_bridge/.darcs_bridge
# Cannot remove master branch...
not darcs-fastconvert branch remove $bridge_path master

# Cannot remove non-existent branch...
not darcs-fastconvert branch remove $bridge_path non-existent

darcs get R S
darcs-fastconvert branch add $bridge_path S

darcs-fastconvert branch list $bridge_path | grep S

darcs-fastconvert branch remove $bridge_path S

not darcs-fastconvert branch list $bridge_path | grep S

cd S
content=XXX_NOEXPORT_XXX
echo $content >> a
darcs rec -am 'edit a'
cd ../

# There should be no changes (this will exit with non-zero if there are)
darcs-fastconvert sync --bridge-path $bridge_path --repo-type darcs
