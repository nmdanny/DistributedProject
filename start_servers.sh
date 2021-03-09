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
tmux new-session -d -s $session
tmux attach-session -t $session


WORK_DIR=$(mktemp -d)

for ((id=0; id < $num_servers; id++))
do
    tmux new-window -a -n "Server $id" "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs --phase_length $phaseLength server --id $id &> $WORK_DIR/s-$id &; less -R $WORK_DIR/s-$id"
done

for ((id=0; id < $num_clients; id++))
do
    tmux new-window -a -n "Client $id" "$exe --num_servers $num_servers -t $threshold --num_clients $num_clients --num_channels $num_channels --timeout $timeoutMs --phase_length $phaseLength client --id $id &> $WORK_DIR/c-$id &; less -R $WORK_DIR/c-$id"
done
