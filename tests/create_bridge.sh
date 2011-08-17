#!/bin/bash

set -ev

rm -rf R R_bridge .darcs_bridge R_git

darcs init --repo R
cd R
echo testing > a && darcs add a && darcs rec -am 'a'
echo also_testing > b && darcs add b && darcs rec -am 'b'
cd ..

cwd=$(pwd)

function pathTest {
    bridgeLoc=$1
    [[ -e $bridgeLoc && -e $bridgeLoc/R && -e $bridgeLoc/R_git && -e $bridgeLoc/.darcs_bridge ]]
    conf="$bridgeLoc/.darcs_bridge/config"
    echo $conf
    dPath=$(grep darcs_path "$conf")
    echo $dPath
    gPath=$(grep git_path "$conf")
    echo $gPath
    # Canonicalise path
    fullBridgePath=$(readlink -f "$cwd/$bridgeLoc")
    echo "$dPath" | grep "$fullBridgePath/R"
    echo "$gPath" | grep "$fullBridgePath/R_git"
}

darcs-fastconvert create-bridge R
pathTest "R_bridge"

darcs-fastconvert create-bridge --clone=no R
pathTest "."
