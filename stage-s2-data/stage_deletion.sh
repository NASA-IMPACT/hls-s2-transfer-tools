#!/bin/bash
doy=2015331
while [ $doy -ge 2015150 ];
do
  echo $doy
  python3 -u query_athena_thread_delete.py $doy
  doy=$(( $doy - 1 ))
done
