#!/bin/bash
if [ $# -ne 1 ]; then
  echo "[ERROR] there is no enough args, you need input name.";
  APP_HOME=`pwd`
  namelist=`ls -l ${APP_HOME}/config/|grep rw|awk -F '.config.yaml' '{print $1}'|awk '{print $9}'`;
  echo "############################"
  echo "[namelist]: "
  echo "${namelist}"
  echo "############################"
  exit 1;
fi
#paramer
v_name=$1
#############################
echo "Stopping ${v_name}"
jstorm kill ${v_name} 5
