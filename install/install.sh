#!/bin/bash

## For now these are the shell commands that work for Linux and Mac. 
## TODO - better shell script with informative errors (see brew's install script as an example) 


curl -s https://api.github.com/repos/kaskada-ai/kaskada/releases/latest |\
grep "browser_download_url.*" |\
grep $(uname -m | sed 's/x86_64/amd64/') |\
grep $(uname -s | tr '[:upper:]' '[:lower:]') |\
cut -d ':' -f2,3 |\
tr -d \" |\
xargs -I {} sh -c 'curl -L {} -o $(basename {}| cut -d '-' -f1,2)'

chmod +x kaskada-*
