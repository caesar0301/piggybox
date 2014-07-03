#!/bin/bash
echo "Packaging classes ... "
mvn package

##-------------##
# Personal use ##
##-------------##
SRC=target/piggybox-*-with-dependencies.jar
PYUDF=py/pyudf.py
BACKUP=/home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/libs

echo "Backuping to $BACKUP ... "
cp $SRC $BACKUP/piggybox.jar
cp $PYUDF $BACKUP/pyudf.py

PIGLIBS=/home/chenxm/.pig/libs
echo "Copying to local pig path: $PIGLIBS ... "
cp $SRC $PIGLIBS/piggybox.jar
cp $PYUDF $PIGLIBS/pyudf.py

echo "Uploading piggybox.jar to Hadoop cluster ... "
scp -P 3022 $SRC $PYUDF \
    chenxm@hadoopjob.omnilab.sjtu.edu.cn:$PIGLIBS/
