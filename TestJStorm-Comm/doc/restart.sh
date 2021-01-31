sh stop.sh EmitHdfs
sleep 6
sh start.sh hdfs
start_result=$?
echo "start_result : ${start_result}"
if [[ ${start_result} -ne 0 ]]; then
  sh start.sh hdfs
  start_result=$?
  echo "re start_result : ${start_result}"
fi
