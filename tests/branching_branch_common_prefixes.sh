#!/bin/bash
read -r -d '' DATA <<'EOF'
blob
mark :1
data 8
testing

reset refs/heads/master
commit refs/heads/master
mark :2
author CommiterName <me@example.org> 1307452813 +0100
committer CommiterName <me@example.org> 1307452813 +0100
data 6
add a
M 100644 :1 a

commit refs/heads/branch1
mark :3
author CommiterName <me@example.org> 1307452821 +0100
committer CommiterName <me@example.org> 1307452821 +0100
data 6
add b
from :2
M 100644 :1 b

commit refs/heads/branch1
mark :4
author CommiterName <me@example.org> 1307452821 +0100
committer CommiterName <me@example.org> 1307452821 +0100
data 6
add c
from :3
M 100644 :1 c

reset refs/heads/branch2
from :3

commit refs/heads/branch2
mark :5
author CommiterName <me@example.org> 1307452821 +0100
committer CommiterName <me@example.org> 1307452821 +0100
data 6
add d
from :3
M 100644 :1 d

commit refs/heads/master
mark :6
author CommiterName <me@example.org> 1307452831 +0100
committer CommiterName <me@example.org> 1307452831 +0100
data 6
add e
from :2
M 100644 :1 e

reset refs/heads/master
from :4

EOF

set -ev
rm -rf T T-head-branch1 T-head-branch2
rm -rf R R-head-T-head-branch1 R-head-T-head-branch2
echo "$DATA" | darcs-fastconvert import --create=yes T

darcs-fastconvert export T T-head-branch1 T-head-branch2 | darcs-fastconvert import R

[[ -e R && -e R-head-T-head-branch1 && -e R-head-T-head-branch2
&& -e R/a && -e R/e && -e R-head-T-head-branch1/b
&& -e R-head-T-head-branch1/c && -e R-head-T-head-branch2/b
&& -e R-head-T-head-branch2/d && (! -e R/b) && (! -e R/c) && (! -e R/d)
&& $(darcs cha --count --repo R-head-T-head-branch1) -eq 3
&& $(darcs cha --count --repo R-head-T-head-branch2) -eq 3 ]]
