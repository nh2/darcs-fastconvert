rm -rf R git-R S

darcs init --repo R
git init git-R

cd R

echo -e '1\n2\n3' > a
darcs add a
darcs rec -am 'add a'
darcs replace 1 4 a
darcs rec -am 'amend a'

echo -e '1\n1\n1' >> a
darcs replace 2 5 a
echo ya | darcs amend

cd ../git-R

darcs-fastconvert export ../R | git fast-import
git fast-export --all | darcs-fastconvert import ../S

[[ $(darcs cha -v --repo ../S | grep -c 'replace') -eq 2 ]]
