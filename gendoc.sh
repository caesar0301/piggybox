#!/bin/bash
# Auto tool to generate java docs and upload it to project branch of gh-pages.
# By chenxm
# chenxm35@gmail.com

set -e # Exit with errors

mvn javadoc:javadoc
ghp-import -p -b gh-pages -m "Update java apidoc." target/site/apidocs/
echo "Updated!"

exit 0;
