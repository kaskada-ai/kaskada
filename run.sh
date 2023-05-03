#!/bin/bash

# make sure the TMPDIR directory exists
mkdir -p $TMPDIR

# Start the first process
/bin/wren &

# Start the second process
/bin/sparrow-main serve &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
