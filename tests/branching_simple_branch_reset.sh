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

reset refs/heads/branch1
from :2

EOF

set -ev
rm -rf R R-head-branch1
echo "$DATA" | darcs-fastconvert import --create=yes R

[[ -e R/a && -e R-head-branch1/a ]]
