#!/bin/bash
############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################################
set -e

echo "Script that assembles all you need to make an RC."
echo "It generates source and binary tar in release directory"
echo "Presumes that you can sign a release as described at https://www.apache.org/dev/release-signing.html"
echo "Starting...";sleep 2s

# Set directory variables
DIR_ROOT="$(cd $(dirname $0);pwd)/.."
cd $DIR_ROOT
PHOENIX="$(xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" pom.xml)"
DIR_REL_BASE=$DIR_ROOT/release
DIR_REL_ROOT=$DIR_REL_BASE/apache-phoenix-$PHOENIX
DIR_REL_BIN=apache-phoenix-$PHOENIX-bin
DIR_REL_BIN_PATH=$DIR_REL_ROOT/$DIR_REL_BIN
REL_SRC=apache-phoenix-$PHOENIX-src
DIR_REL_SRC_TAR_PATH=$DIR_REL_ROOT/src
DIR_REL_BIN_TAR_PATH=$DIR_REL_ROOT/bin
DIR_BIN=$DIR_REL_BIN_PATH/bin
DIR_PHERF_CONF=phoenix-pherf/config
DIR_EXAMPLES=$DIR_REL_BIN_PATH/examples
DIR_DOCS=dev/release_files

# Verify no target exists
mvn clean; rm -rf $DIR_REL_BASE;
RESULT=$(find -iname target)

if [ -z "$RESULT" ]
then
  echo "Verified target directory does not exist.";
else
  echo "Target directory exists at: $RESULT. Please use a clean repo.";
  exit -1;
fi

# Generate src tar
ln -s . $REL_SRC; tar cvzf $REL_SRC.tar.gz --exclude="$REL_SRC/$REL_SRC" $REL_SRC/*; rm $REL_SRC;

# Generate directory structure
mkdir $DIR_REL_BASE;
mkdir $DIR_REL_ROOT;
mkdir $DIR_REL_BIN_PATH;
mkdir $DIR_REL_BIN_TAR_PATH;
mkdir $DIR_REL_SRC_TAR_PATH;
mkdir $DIR_EXAMPLES;
mkdir $DIR_BIN;

# Move src tar
mv $REL_SRC.tar.gz $DIR_REL_SRC_TAR_PATH;

# Copy common jars
mvn clean apache-rat:check package -DskipTests -Dcheckstyle.skip=true -q;
rm -rf $(find . -type d -name archive-tmp);

# Copy all phoenix-*.jars to release dir
phx_jars=$(find -iwholename "./*/target/phoenix-*.jar")
cp $phx_jars $DIR_REL_BIN_PATH;

# Copy bin
cp bin/* $DIR_BIN;
cp -R $DIR_PHERF_CONF $DIR_BIN;

# Copy release docs

cp $DIR_DOCS/* $DIR_REL_BIN_PATH;

# Copy examples
cp -r examples/* $DIR_EXAMPLES

# Generate bin tar
tar cvzf $DIR_REL_BIN_TAR_PATH/$DIR_REL_BIN.tar.gz -C $DIR_REL_ROOT apache-phoenix-$PHOENIX-bin;
rm -rf $DIR_REL_BIN_PATH;

echo "DONE generating binary and source tars in release directory."
echo "Now signing source and binary tars"

# Sign
function_sign() {
  phoenix_tar=$(find apache-phoenix-*.gz);
  gpg --armor --output $phoenix_tar.asc --detach-sig $phoenix_tar;
  md5sum -b $phoenix_tar > $phoenix_tar.md5;
  sha512sum -b $phoenix_tar > $phoenix_tar.sha;
  sha256sum -b $phoenix_tar >> $phoenix_tar.sha;
}

cd $DIR_REL_BIN_TAR_PATH; function_sign;
cd $DIR_REL_SRC_TAR_PATH; function_sign;

# Tag
read -p "Do you want add tag for this RC in GIT? (Y for yes or any other key to continue)" prompt
if [[ $prompt =~ [yY](es)* ]]
then
  echo "Tagging..."
  read -p "Enter tag (Example 5.0.0-rc0):" prompt
  echo "Setting tag: $prompt";sleep 5s
  git tag -a $prompt -m "$prompt"; git push origin $prompt
  mv $DIR_REL_ROOT $DIR_REL_BASE/phoenix-$prompt
fi

echo "DONE."
echo "If all looks good in release directory then commit RC at https://dist.apache.org/repos/dist/dev/phoenix"
