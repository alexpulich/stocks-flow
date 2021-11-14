#!/bin/sh
for filename in flows/*.py; do
  echo $filename
  python $filename
done