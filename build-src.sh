#!/bin/sh

VERSION=`grep "VERSION =" src/smdba/smdba | sed s'/^.*"\([[:digit:]\.]\+\)"/\1/'`
NAME="smdba"

# Update
#git pull

# Cleanup
rm -f $NAME-*.tar.bz2
find . | grep '.pyc$' | xargs rm 2>/dev/null
find . | grep '~$' | xargs rm 2>/dev/null

# Archive
mkdir $NAME-$VERSION
cp -rv src $NAME-$VERSION/
cp -rv doc $NAME-$VERSION/
cp -v LICENSE README.md $NAME-$VERSION/
cp -v setup.py $NAME-$VERSION/

tar cvf - $NAME-$VERSION | bzip2 > $NAME-$VERSION.tar.bz2
rm -rf $NAME-$VERSION

echo
echo "Done"
echo
