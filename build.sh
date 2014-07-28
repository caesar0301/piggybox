#!/bin/bash
echo "Packaging classes ... "
mvn package

##-------------##
# Personal use ##
##-------------##
SRC=target/piggybox-*-with-dependencies.jar
PYUDF=src/main/python/pyudf.py

PIGLIBS=/home/chenxm/.pig/libs
echo "Copying to local pig path: $PIGLIBS ... "
cp $SRC $PIGLIBS/piggybox.jar
cp $PYUDF $PIGLIBS/pyudf.py

echo "Uploading piggybox.jar to Hadoop cluster ... "
scp -P 3022 $SRC $PYUDF \
    chenxm@hadoopjob.omnilab.sjtu.edu.cn:$PIGLIBS/
