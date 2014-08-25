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
PHOENIX="$(xpath -q -e '/project/version/text()' pom.xml)"
DIR_REL_BASE=$DIR_ROOT/release
DIR_REL_ROOT=$DIR_REL_BASE/phoenix-$PHOENIX
DIR_REL_BIN=phoenix-$PHOENIX-bin
DIR_REL_BIN_PATH=$DIR_REL_ROOT/$DIR_REL_BIN
REL_SRC=phoenix-$PHOENIX-src
DIR_REL_SRC_TAR_PATH=$DIR_REL_ROOT/src
DIR_REL_BIN_TAR_PATH=$DIR_REL_ROOT/bin
DIR_BIN=bin
DIR_EXAMPLES=$DIR_REL_BIN_PATH/examples
DIR_COMMON=$DIR_REL_BIN_PATH/common
DIR_HADOOP=$DIR_REL_BIN_PATH/hadoop
DIR_DOCS=dev/release_files

# Verify no target exists
mvn clean; mvn clean -Dhadoop.profile=2; rm -rf $DIR_REL_BASE;
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
mkdir $DIR_COMMON;

# Move src tar
mv $REL_SRC.tar.gz $DIR_REL_SRC_TAR_PATH;

# Copy common jars
mvn clean apache-rat:check package -DskipTests;
rm -rf $(find . -type d -name archive-tmp);
cp $(find -iname phoenix-$PHOENIX-client-minimal.jar) $DIR_COMMON;
cp $(find -iname phoenix-$PHOENIX-client-without-hbase.jar) $DIR_COMMON;
cp $(find -iname phoenix-core-$PHOENIX.jar) $DIR_COMMON;

# Copy release docs
function_copy() {
  if [ ! -f $1 ];
   then
    echo ""; echo "ERROR!! Please check-in $1 file before running make_rc.sh"; 
    exit -1; 
  fi
  cp $1 $DIR_REL_BIN_PATH;
}

function_copy CHANGES;
function_copy DISCLAIMER;
function_copy README;
cp $DIR_DOCS/* $DIR_REL_BIN_PATH;

# Copy examples
cp -r examples/* $DIR_EXAMPLES

# Copy hadoop specific jars
function_copy_hadoop_specific_jars() {
  mkdir $DIR_HADOOP$1;
  mkdir $DIR_HADOOP$1/$DIR_BIN;
  cp $DIR_BIN/* $DIR_HADOOP$1/$DIR_BIN;
  cp $(find -iname phoenix-$PHOENIX-client.jar) $DIR_HADOOP$1/phoenix-$PHOENIX-client-hadoop$1.jar;
  cp $(find -iname phoenix-$PHOENIX-server.jar) $DIR_HADOOP$1/phoenix-$PHOENIX-server-hadoop$1.jar;
  cp $(find -iname phoenix-core-$PHOENIX-tests.jar) $DIR_HADOOP$1/phoenix-core-$PHOENIX-tests-hadoop$1.jar;
  cp $(find -iname phoenix-flume-$PHOENIX.jar) $DIR_HADOOP$1/phoenix-flume-$PHOENIX-hadoop$1.jar;
  cp $(find -iname phoenix-flume-$PHOENIX-tests.jar) $DIR_HADOOP$1/phoenix-flume-$PHOENIX-tests-hadoop$1.jar;
  cp $(find -iname phoenix-pig-$PHOENIX.jar) $DIR_HADOOP$1/phoenix-pig-$PHOENIX-hadoop$1.jar;
  cp $(find -iname phoenix-pig-$PHOENIX-tests.jar) $DIR_HADOOP$1/phoenix-pig-$PHOENIX-tests-hadoop$1.jar;
}
function_copy_hadoop_specific_jars 1;
mvn clean package -Dhadoop.profile=2 -DskipTests;
function_copy_hadoop_specific_jars 2;

# Generate bin tar
tar cvzf $DIR_REL_BIN_TAR_PATH/$DIR_REL_BIN.tar.gz -C $DIR_REL_ROOT phoenix-$PHOENIX-bin;
rm -rf $DIR_REL_BIN_PATH;

echo "DONE generating binary and source tars in release directory."
echo "Now signing source and binary tars"

# Sign
function_sign() {
  phoenix_tar=$(find phoenix-*.gz);
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
