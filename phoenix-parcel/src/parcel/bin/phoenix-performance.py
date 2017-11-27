#!/bin/bash
  # Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
  SOURCE="${BASH_SOURCE[0]}"
  BIN_DIR="$( dirname "$SOURCE" )"
  while [ -h "$SOURCE" ]
  do
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  done
  BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  LIB_DIR=$BIN_DIR/../lib


# Autodetect JAVA_HOME if not defined
. $LIB_DIR/../../CDH/lib/bigtop-utils/bigtop-detect-javahome

export PATH=$JAVA_HOME/jre/bin:$PATH
exec $LIB_DIR/phoenix/bin/performance.py "$@"
