rm -rf R
mkdir R
cd R

git init
darcs init

yes | head -n 20 > a

echo -e 'z\ny\nx' > c

git add a c
git commit -m 'Add a and c'

mkdir foo
# Let's make sure we handle filenames with spaces.
touch 'foo/bar baz'

echo -e 'a\nb\nc' > b

# Add a z at the beginning, middle and end of 'a'.
echo z > temp_a
yes | head -n 10 >> temp_a
echo z >> temp_a
yes | head -n 10 >> temp_a
mv temp_a a
echo 'z' >> a

git add a b 'foo/bar baz'
git rm c
# A commit that tests all the different types of diff output we may see.
git commit -m 'Change a, add b, remove c, and add "bar baz"'

# Clean up, so Darcs doesn't complain when we import the patch.
rm -rf a b c foo

git format-patch --stdout --all > patch
darcs-fastconvert apply-patch . patch

[[ $(darcs cha --count) -eq 2 && -e a && $(wc -l a | awk '{print $1}') -eq 23
   && (! -e c) && -e b && -e foo && -e 'foo/bar baz' ]]
