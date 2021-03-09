#!/bin/bash


num_servers=5
threshold=3
num_clients=3
num_channels=30

phaseLength=500
timeoutMs=1000


exe="./target/release/runner"

session="DistLib"

tmux set -g remain-on-exit on
tmux kill-session -t $session
tmux new-session -d -s $session


WORK_DIR=$(mktemp -d)

for ((id=0; id < $num_servers; id++))
do
    tmux new-window -a -n "Server $id" "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs server --id $id > $WORK_DIR/s-$id & tail -f $WORK_DIR/s-$id"
done

for ((id=0; id < $num_clients; id++))
do
    tmux new-window -a -n "Client $id" "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs client --id $id > $WORK_DIR/c-$id & tail -f $WORK_DIR/c-$id"
done


tmux attach-session -t $session