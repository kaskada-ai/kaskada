#!/bin/bash

# make sure the TMPDIR directory exists
mkdir -p $TMPDIR


# Start the first process
DB_DIALECT=$DB_DIALECT DB_PATH=$DB_PATH DB_IN_MEMORY=$DB_IN_MEMORY \
OBJECT_STORE_TYPE=$OBJECT_STORE_TYPE OBJECT_STORE_PATH=$OBJECT_STORE_PATH OBJECT_STORE_BUCKET=$OBJECT_STORE_BUCKET \
/bin/wren &

# Start the second process
/bin/sparrow-main serve &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
