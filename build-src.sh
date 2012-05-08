#!/bin/sh

VERSION="1.0"
NAME="sm-db"

# Update
#git pull

# Cleanup
rm -rf $NAME-$VERSION
find . | grep '.pyc$' | xargs rm 2>/dev/null
find . | grep '~$' | xargs rm 2>/dev/null

# Archive
pushd src
cp -rv . ../$NAME-$VERSION
popd

tar cvf - $NAME-$VERSION | bzip2 > $NAME-$VERSION.tar.bz2
rm -rf $NAME-$VERSION

echo
echo "Done"
echo
