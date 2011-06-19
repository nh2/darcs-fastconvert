#!/bin/bash
read -r -d '' DATA <<'EOF'
progress 1: a
commit refs/heads/master
mark :1
committer Owen Stephens <darcs@owenstephens.co.uk> 1308486283 +0000
data 6
add a
M 100644 inline a
data 6
1
2
3

EOF

set -ev
rm -rf R
echo "$DATA" | darcs-fastconvert import --create=yes R
cd R

[[ -e a && $(wc -l a | sed -e 's/\([0-9]\+\).*/\1/') -eq 3 ]]
