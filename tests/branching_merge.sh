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

commit refs/heads/master
mark :3
author Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
data 9
b master
from :2
M 100644 :1 b

blob
mark :4
data 6
a
b
c

commit refs/heads/branch1
mark :5
author Owen Stephens <git@owenstephens.co.uk> 1308064734 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064734 +0100
data 10
b branch1
from :2
M 100644 :4 b
M 100644 :4 c

blob
mark :6
data 4
1
c

commit refs/heads/master
mark :7
author Owen Stephens <git@owenstephens.co.uk> 1308064774 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064774 +0100
data 38
Merge branch 'branch1'

Conflicts:
	b
from :3
merge :5
M 100644 :6 b

reset refs/heads/master
from :7

EOF

set -ev
rm -rf R R-branch_branch1
echo "$DATA" | darcs-fastconvert import --create=yes R

[[ -e R && -e R/a && -e R/b && -e R/c ]]
