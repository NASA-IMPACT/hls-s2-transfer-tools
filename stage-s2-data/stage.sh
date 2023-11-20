#!/bin/bash
doy=2019365
while [ $doy -ge 2016001 ];
do
  echo $doy
  python3 -u query_athena_thread.py $doy
  doy=$(( $doy - 1 ))
done
