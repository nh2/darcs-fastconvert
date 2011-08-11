#!/bin/bash

set -ev
. lib

rm -rf R S

darcs init --repo R
cd R

echo -e '1\n2\n3' > a && darcs add a && darcs rec -am 'Add a'
darcs get . ../S

echo -e '1\n2\n3' > b && darcs add b && darcs rec -am 'Add b'
echo -e '1\n2\n3' > c && darcs add c && darcs rec -am 'Add c'
darcs tag -m 'darcs-fastconvert merge pre-source: foomerge'

cd ../S

echo -e 'a\nb\nc' > b && darcs add b && darcs rec -am 'again Add b'
echo -e 'a\nb\nc' > c && darcs add c && darcs rec -am 'again Add c'

darcs tag -m 'darcs-fastconvert merge pre-target: foomerge'

darcs pull -a ../R

echo -e 'z\nx\nc' > b && darcs rec -am 'resolve b'
echo -e 'z\nx\nc' > c && darcs rec -am 'resolve c'

darcs tag -m 'darcs-fastconvert merge post: foomerge'

# Ensure that exporting *only* the target branch works (patches need to be
# commuted to obtain their pre-merge form).
darcs-fastconvert export .

cd ..
# Ensure that exporting both the source and target branch works (no patch
# commutation required - the patches are available.)
darcs-fastconvert export S R
