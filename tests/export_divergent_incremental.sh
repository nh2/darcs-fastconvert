set -ev
rm -rf R S T T-head-S import_marks export_marks

darcs init --repo R

cd R
echo -e '1\n2\n3' > a
darcs add a
darcs rec -am 'add a'
cd ..

darcs-fastconvert export --write-marks export_marks R | darcs-fastconvert import --write-marks import_marks T

darcs get -q R S
cd R
echo -e '1\n2\n4' > a
darcs rec -am 'change a R'
cd ../S
echo -e '1\n2\n5' > a
darcs rec -am 'change a S'
cd ..

darcs-fastconvert export --read-marks export_marks R S | darcs-fastconvert import --create=no --read-marks import_marks T
