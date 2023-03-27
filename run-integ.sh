#!/bin/bash

# make sure the TMPDIR directory exists
mkdir -p $TMPDIR

# Start wren 
/bin/wren &

# Start the second process
/bin/sparrow-main serve &

