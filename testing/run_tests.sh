#!/bin/bash

# dd if=/dev/urandom of=data bs=4096 count=400

directory="/cs/scratch/saaz/mnt"

if [ -z "$1" ]; then
    directory="/cs/scratch/saaz/mnt"
else
    directory="$1"
fi

G='\033[0;32m'
R='\033[0;31m'
N='\033[0m'

tpass=0
tests="$(ls -1q tests/*.sh | wc -l)"


echo "1. Running $tests tests"
echo ""


for t in tests/*.sh; do
    
    if bash "$t" $directory; then
	printf "   ${G}Passed${N} [${G}✔${N}] $t\n"
	((tpass++))
    else
	printf "   ${R}Failed${N} [${R}✘${N}] $t\n"
    fi

done

echo ""
echo "   Successfully Passed $tpass out of $tests tests"



