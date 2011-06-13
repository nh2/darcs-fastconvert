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

commit refs/heads/master
mark :4
author CommiterName <me@example.org> 1307452831 +0100
committer CommiterName <me@example.org> 1307452831 +0100
data 6
add c
from :2
M 100644 :1 c

reset refs/heads/master
from :4

EOF

set -ev
rm -rf R R-branch_branch1
echo "$DATA" | darcs-fastconvert import --create=yes R

[[ -e R && -e R-branch_branch1 && -e R/a && -e R/c && -e R-branch_branch1/b &&
    (! -e R/b) ]] 
