#!/bin/bash
. lib
set -ev
! read -r -d '' DATA <<'EOF'
blob
mark :1
data 8
testing

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

commit refs/heads/branch1
mark :4
author Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
data 10
z branch1
from :3
M 100644 :1 z

EOF

! read -r -d '' DATA2 <<'EOF'
blob
mark :5
data 9
testing2

commit refs/heads/master
mark :6
author Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
data 9
c master
from :2
M 100644 :5 c

reset refs/heads/branch2
from refs/heads/branch1

commit refs/heads/branch1
mark :7
author Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064748 +0100
data 10
d branch1
from :3
M 100644 :5 d

EOF

rm -rf R R-head-branch1 marks
echo "$DATA" | darcs-fastconvert import --debug --write-marks=marks R
echo "$DATA2" | darcs-fastconvert import --debug --read-marks=marks --create=no R

echo -e '100: f00 head-branch1\n101: 20110614151834-5c01c-91ed99d02b622ea62b1cbc44f21d7be4328416c4.gz invalid_branch' > marks

! read -r -d '' FAIL_BRANCH_DATA <<'EOF'
blob
mark :22
data 8
testing

commit refs/heads/foobar1
mark :23
author Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
data 9
q master
from :101
M 100644 :22 q

EOF

# Catch invalid branch name
not echo "$FAIL_BRANCH_DATA" | darcs-fastconvert import --debug --read-marks=marks --create=no R

! read -r -d '' FAIL_HASH_DATA <<'EOF'
blob
mark :20
data 8
testing

commit refs/heads/foobar2
mark :21
author Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
committer Owen Stephens <git@owenstephens.co.uk> 1308064714 +0100
data 9
q master
from :100
M 100644 :20 q

EOF

# Catch invalid hash
not echo "$FAIL_HASH_DATA" | darcs-fastconvert import --debug --read-marks=marks --create=no R
