#!/bin/bash
# Run replication tests in a loop to detect flakes. Stops on first failure.
# Usage: bash docs/run-tests-in-loop.sh [num_iterations] [test_classes]

ITERATIONS=${1:-20}
TESTS=${2:-"ReplicationLogGroupTest,ReplicationLogDiscoveryForwarderTest"}
mvn install -pl phoenix-core -am -DskipTests=true
CMD="mvn test -pl phoenix-core -Dtest=\"$TESTS\" -q"
echo "Running $CMD in a loop of $ITERATIONS"

caffeinate -i bash -c "for i in \$(seq 1 $ITERATIONS); do echo \"=== Run \$i ===\"; date; if ! $CMD; then echo \"FAILED on run \$i\"; break; fi; done"
