#!/usr/bin/bash

cleanup() {
    /opt/ood/nginx_stage/sbin/nginx_stage nginx --user $1 -s stop
    echo STOP CALLED
    exit 0
}

trap 'cleanup' TERM INT

/opt/ood/nginx_stage/sbin/nginx_stage pun -u $1
sleep 9000000 &
wait
