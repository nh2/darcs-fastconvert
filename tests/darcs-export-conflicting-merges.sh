
rm -rf master branch1 branch2 gitrepo

darcs init --repo master

cd master
echo a > a && darcs add a 
echo 'before merges' > b && darcs add b
darcs rec -am 'Initial state'

cd ..

darcs get master branch1
darcs get master branch2

cd branch1
echo branch1 > b && darcs rec -am 'Add b branch1'
darcs tag -m 'darcs-fastconvert merge pre-source: foo_merge_id'

cd ../branch2
echo branch2 > b && darcs rec -am 'Add b branch2'
darcs tag -m 'darcs-fastconvert merge pre-source: foo_merge_id'

cd ../master
echo master > b && darcs rec -am 'Add b master'
darcs tag -m 'darcs-fastconvert merge pre-target: foo_merge_id'

darcs pull -a ../branch1

darcs pull -a ../branch2

darcs rev -a

echo 'master resolution' > b && darcs rec -am 'Resolve b conflict in master'

darcs tag -m 'darcs-fastconvert merge post: foo_merge_id'

cd ..

git init gitrepo

darcs-fastconvert export master branch1 branch2 | (cd gitrepo && git fast-import)

cd gitrepo
git reset --hard

[[ $(head b) == 'master resolution' ]]

git checkout branch1

[[ $(head b) == 'branch1' ]]

git checkout branch2

[[ $(head b) == 'branch2' ]]

git checkout master~1

[[ $(head b) == 'master' ]]

git checkout master~2

[[ $(head b) == 'before merges' ]]
