#!/bin/sh

# This script replaces the normal sources with the ones where the packages are replaced for HBase 3.0
# The purpose is to enable debugging from an IDE without having to change the source directory settings
# Make sure to never commit the changes this script makes.

# Usage:
# 1. Activate the Hadoop3 maven profile in your ide (and deactive the default)
# 2. Make sure that you have no uncommitted changes
# 3. Run "mvn clean package -am -pl phoenix-core -Dhbase.profile=3.0 -DskipTests"
# 4. Run this script
# 5. Work with the source in the IDE
# 6. get a diff of your fixes 
# 7. Run "git reset --hard"
# 8. Re-apply your changes.

orig_dir=$(pwd)
cd "$(dirname "$0")"/..
cp -r phoenix-core/target/generated-sources/replaced/* phoenix-core/src
cp -r phoenix-core-client/target/generated-sources/replaced/* phoenix-core-client/src
cp -r phoenix-core-server/target/generated-sources/replaced/* phoenix-core-server/src

cd $orig_dir
