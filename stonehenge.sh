#!/bin/bash

source /opt/env/latest.sh
LD_LIBRARY_PATH+=:/usr/lib64
/home/trigger/stonehenge/stonehenge $*
