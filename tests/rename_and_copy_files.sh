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

commit refs/heads/master
mark :3
author CommiterName <me@example.org> 1307452821 +0100
committer CommiterName <me@example.org> 1307452821 +0100
data 19
Copy, rename, copy
from :2
C "a" "c"
R "a" "b"
C "b" "d"

EOF

set -ev
rm -rf R
echo "$DATA" | darcs-fastconvert import --create=yes R
cd R
[[ -e c && -e b && -e d && ! -e a ]]
[[ $(darcs cha --count) -eq 2 ]]
