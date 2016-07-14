#!/usr/bin/env bash

worker_cnt=8

case $1 in
    start|stop|restart|eval)
        ;;
    *)
        echo "bad command '$1', expected 'start'/'stop'/'restart'"
        exit -1;
        ;;
esac

case $2 in
    '')
        ;;
    *[!0-9]*)
        echo "error: bad number of Tarantool instances '$2', expecting number"
        exit -1
        ;;
    *)
        worker_cnt=$2
        ;;
esac

echo "${1}ing ${worker_cnt} tarantool instances"

for i in `seq 1 ${worker_cnt}`; do
    if [ ! -f worker-${i}.lua ]; then
        ln -s common.lua worker-${i}.lua
    fi
    tarantoolctl $1 worker-$i $3
    if [ ! $? -eq 0 ]; then
        printf "Failed to '%s' tarantool instance '%s'\n" "$1" "worker-$i"
        exit 1
    fi
    if [[ "$1" == "start" || "$1" == "restart" ]]; then
        printf "Waiting Tarantool to start... "
        ./wait_launched.py worker-$i
        if [ ! $? -eq 0 ]; then
            printf "failed\n"
            exit 1
        fi
        printf "done\n"
    fi
done
exit 0
