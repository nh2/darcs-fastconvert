set -ev

rm -rf foo foo_bridge

darcs init --repo foo
cd foo
echo -e '1\n2' > a
darcs add a
darcs rec -am 'add a'
cd ..

darcs-fastconvert create-bridge foo

# Create a git branch at the current state.
(cd foo_bridge/foo_git && git branch foo1)

darcs-fastconvert branch track --branch-type git foo_bridge foo1

[[ -e foo_bridge/foo-head-foo1/a
   && $(darcs cha --repo foo_bridge/foo-head-foo1 --count) -eq 1 ]]
