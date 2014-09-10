#!/bin/bash

source ~/rat_env.sh
LD_LIBRARY_PATH+=:/home/cp/klabe/hiredis/lib
LD_LIBRARY_PATH+=:/home/cp/klabe/chopper/SFMT-src-1.4.1
/home/cp/klabe/chopper/chopper $*
