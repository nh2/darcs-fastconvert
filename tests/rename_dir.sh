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
data 10
add dir/a
M 100644 :1 dir/a

commit refs/heads/master
mark :3
author CommiterName <me@example.org> 1307452821 +0100
committer CommiterName <me@example.org> 1307452821 +0100
data 24
copy, rename, copy dirs
from :2
C "dir" "dir2"
R "dir" "dir3"
C "dir3" "dir4"

EOF

set -ev
rm -rf R
echo "$DATA" | darcs-fastconvert import --create=yes R
cd R
[[ -e dir2 && -e dir3 && -e dir4 && (! -e dir) && -e dir2/a && -e dir3/a && -e dir4/a ]]
[[ $(darcs cha --count) -eq 2 ]]
