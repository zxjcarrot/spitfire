#!/bin/sh

FILES=`/bin/ls -l *.test 2>/dev/null`

if [ -z "$FILES" ]; then
    echo no test to perform
else
    echo `for i in *.test; do ./$i; done | grep passed | wc -l` success
    echo `/bin/ls -l *.test | wc -l` in all
fi
