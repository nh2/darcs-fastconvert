#!/bin/bash
read -r -d '' DATA <<'EOF'
blob
mark :1
data 6
1
2
3

reset refs/heads/master
commit refs/heads/master
mark :2
author Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
data 9
a master
M 100644 :1 a

commit refs/heads/branch1
mark :3
author Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
data 10
b branch1
from :2
M 100644 :1 b

commit refs/heads/branch2
mark :4
author Owen Stephens <git@owenstephens.co.uk> 1308064734 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064734 +0100
data 10
c branch2
from :2
M 100644 :1 c

commit refs/heads/master
mark :5
author Owen Stephens <git@owenstephens.co.uk> 1308064774 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064774 +0100
data 26
Merge branch1 and branch2
from :2
merge :3
merge :4

reset refs/heads/master
from :5

EOF

set -ev
rm -rf R R-head-branch1 R-head-branch2
echo "$DATA" | darcs-fastconvert import --create=yes R

[[ -e R && -e R/a && -e R/b && -e R/c && -e R-head-branch1/a && -e R-head-branch1/b && (! -e R-head-branch1/c) && -e R-head-branch2/a && -e R-head-branch2/c && (! -e R-head-branch2/b) ]]
