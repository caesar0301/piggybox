#!/bin/bash
SRC=target/piggybox-*-with-dependencies.jar
DEST=/home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/lib/piggybox.jar

echo "Packaging classes ... "
mvn package
BACKUP=/home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/libs

echo "Backuping to $BACKUP ... "
cp target/piggybox-*-with-dependencies.jar $BACKUP/piggybox.jar
cp py/pyudf.py $BACKUP/pyudf.py

PIGLIBS=/home/chenxm/.pig/libs
echo "Copying to local pig path: $PIGLIBS ... "
cp target/piggybox-*-with-dependencies.jar $PIGLIBS/piggybox.jar
cp py/pyudf.py $PIGLIBS/pyudf.py

echo "Uploading piggybox.jar to Hadoop cluster ... "
scp -P 3022 target/piggybox-*-with-dependencies.jar chenxm@hadoopjob.omnilab.sjtu.edu.cn:$PIGLIBS/piggybox.jar
