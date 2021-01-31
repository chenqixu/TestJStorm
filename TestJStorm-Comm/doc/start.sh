#!/bin/bash
APP_HOME=`pwd`
if [ $# -ne 1 ]; then
  echo "[ERROR] there is no enough args, you need input name.";
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
JSTORM_HOME="/bi/sysapp/jstorm-2.1.1"
APP_MAINCLASS=com.cqx.jstorm.comm.base.SubmitTopology
APP_CONF="$APP_HOME/config/${v_name}.config.yaml"
APP_JAR="$APP_HOME/jarpath"
CLASSPATH=$JSTORM_HOME/jstorm-core-2.1.1.jar
for i in $JSTORM_HOME/lib/*.jar; do
  CLASSPATH="$CLASSPATH":"$i"
done
for i in "$APP_JAR"/*.jar; do
  CLASSPATH="$CLASSPATH":"$i"
done

stormTaskName=${v_name}
maxRetryTime=3
retryTime=0
while true
do
        cnt=`jstorm list | grep "\"name\": \"$stormTaskName\"" | wc -l`
        if (( $cnt > 0 ));then
                echo "$stormTaskName还未停止，休眠10秒"
                sleep 10
                retryTime=`expr $retryTime + 1`
                if(( retryTime == maxRetryTime ));then
                 echo "停止超时，$stormTaskName启动失败"
                 exit -1
                fi
        else
                 break
        fi
done

echo "Starting $APP_CONF $stormTaskName"
$JAVA_HOME/bin/java -classpath $CLASSPATH $APP_MAINCLASS --conf $APP_CONF --type submit --jarpath $APP_JAR

