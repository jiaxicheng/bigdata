#!/bin/bash
##########################################################
# A bash script to manage zookeeper cluster              #
# the script relys on the zoo.cfg file to find all nodes #
# and then run the ssh remote command accordingly        #
# copy this script to $ZK_HOME/bin folder and then run   #
#                                                        #
#     $ZK_HOME/bin/zkServer.sh start                     #
#                                                        #
##########################################################
opt=${1?"Usage: $0 {start|stop|restart}"}
conf_file=$ZK_HOME/conf/zoo.cfg
cmd_to_exec="$ZK_HOME/bin/zkServer.sh $opt"

svr=(`awk -F'[:=]' '$1~/^server\.[0-9]+/{print $2}' "$conf_file"`)

(( ${#svr[@]} )) || { echo "no zookeeper server found..."; exit; }

for s in "${svr[@]}"; do
    echo "$opt zookeeper on $s..."
    [[ $s = "$HOSTNAME" ]] && { $cmd_to_exec >/dev/null 2>&1; continue; }
    ssh -n $s "$cmd_to_exec"  >/dev/null 2>&1
done
