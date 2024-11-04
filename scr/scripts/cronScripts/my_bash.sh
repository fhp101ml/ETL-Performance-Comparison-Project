#!/bin/bash
now=$(date)
echo "hi:  we are here from ${now}"
echo "i can schedule everyting i want even change this bash to a python Script"
# echo printenv
#cron -l 2 -f
printenv > /etc/environment