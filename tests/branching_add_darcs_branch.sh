set -ev

rm -rf foo foo1 foo_bridge

darcs init --repo foo
cd foo
echo -e '1\n2' > a
darcs add a
darcs rec -am 'add a'
cd ..
darcs get -q foo foo1

darcs-fastconvert create-bridge foo

darcs-fastconvert branch track foo_bridge foo1
