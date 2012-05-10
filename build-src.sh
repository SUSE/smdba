#!/bin/sh

VERSION="1.0"
NAME="smdba"

# Update
git pull

# Cleanup
rm -rf $NAME-$VERSION
find . | grep '.pyc$' | xargs rm 2>/dev/null
find . | grep '~$' | xargs rm 2>/dev/null

# Archive
pushd src
cp -rv . ../$NAME-$VERSION
popd
cp -v LICENSE README $NAME-$VERSION/

tar cvf - $NAME-$VERSION | bzip2 > $NAME-$VERSION.tar.bz2
rm -rf $NAME-$VERSION

echo
echo "Done"
echo
