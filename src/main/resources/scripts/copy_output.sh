#!/bin/sh
echo "$1"
if [ -d "$1" ]; then
  rm -r $1
fi
mkdir -p $1
cp -R /opt/workspace/data/temp_output/* $1
# Deleting temp_output directory post copying the data
# rm -r /opt/workspace/data/temp_output/