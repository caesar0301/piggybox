#!/bin/bash
SRC=target/piggybox-*-with-dependencies.jar
DEST=/home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/lib/piggybox.jar

echo "Packaging classes ... "
mvn package
echo "Copying $SRC to $DEST ... "
cp target/piggybox-*-with-dependencies.jar /home/chenxm/tera/workspace/omnilab-misc/sjtuwifi/lib/piggybox.jar
