#!/bin/bash

DATE=`data -d "$1 -2 days" +%Y%m%d`
YESTERDAY=`date -d "$1 -3 days"+%Y%m%d`