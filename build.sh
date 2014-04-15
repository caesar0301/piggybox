#!/bin/bash
SRC=target/piggybox-*-with-dependencies.jar
DEST=/home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/lib/piggybox.jar

echo "Packaging classes ... "
mvn package
echo "Copying to omnilab-misc/sjtuwifi/lib/ ... "
cp target/piggybox-*-with-dependencies.jar /home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/lib/piggybox.jar
echo "Copying to local pib libs path ... "
cp target/piggybox-*-with-dependencies.jar /home/chenxm/.pig/libs/piggybox.jar
echo "Uploading piggybox.jar to Hadoop cluster ... "
scp -P 3022 target/piggybox-*-with-dependencies.jar chenxm@hadoopjob.omnilab.sjtu.edu.cn:/home/chenxm/.pig/libs/piggybox.jar
