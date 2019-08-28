#!/bin/bash

echo -n "" > $1 
for filename in ./tests/*; do \
        echo -n -e "$filename\t" >> $1; \
        /usr/bin/time -f "%e" -o $1 -a sudo -u postgres psql -f $filename >/dev/null; \
done