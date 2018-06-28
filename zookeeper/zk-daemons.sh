#!/bin/bash
##########################################################
# A bash script to manage zookeeper cluster              #
# by Xicheng Jia Spring 2017                             #
# the script relys on the zoo.cfg file to find all nodes #
# and then run the ssh remote command accordingly        #
# copy this script to $ZK_HOME/bin folder and then run   #
#                                                        #
#     $ZK_HOME/bin/zkServer.sh start                     #
#                                                        #
# tested on: zookeeper 3.4.11
##########################################################
opt=${1?"Usage: $0 {start|stop|restart}"}
conf_file=$ZK_HOME/conf/zoo.cfg

# retrieve the datadir, note: no trailing whitespaces is allowed
datadir=$(awk -F= '$1 == "dataDir"{print $2}' "$conf_file")

# retrieve id:server, so 'id' must be integer and server is a word from chars [a-zA-Z0-9_]
while read -r id server; do
    echo -n "'${opt}'ing zookeeper on $server..."
    # make sure myid file exist under dataDir with the correct id
    if [[ $opt == "start" ]]; then
        cmd_to_exec="mkdir -p '$datadir' && { echo $id >'$datadir'/myid; '$ZK_HOME'/bin/zkServer.sh $opt; }"
    else
        cmd_to_exec="$ZK_HOME/bin/zkServer.sh $opt"
    fi

    # run the command either on local or the remote servers
    # multiple commands from a bash variable running at local must be eval'ed
    if [[ $server = "$HOSTNAME" ]]; then
        eval "$cmd_to_exec" >/dev/null 2>&1
    else
        ssh -n $server "$cmd_to_exec"  >/dev/null 2>&1
    fi
    [[ $? -eq 0 ]] || issue+=($server)
    # check the server status
    printf "\t<-- $server: $(nc $server 2181 <<<ruok 2>/dev/null || echo down)\n"
done < <( perl -lne '/^server\.(\d+)=(\w+)/ && print "$1 $2"' "$conf_file" )

# any issue detected list the servers
(( ${#issue[@]} )) && echo "ISSUEs detected, please check servers: $(IFS=,;echo "${issue[*]}")."
