#!/bin/bash

set -eux

WIN=srv0
WORK_DIR=/tmp/frugalos_test/
MODE=debug
RPC_PORT=14278
PORT=3100
# 追加で必要なサーバー数(クラスタ初期化用に1つは必ず作られる)
# Note: 1 <= SERVER_COUNT
SERVER_COUNT=${SERVER_COUNT:-2}
# 各サーバーに追加するデバイス数
# Note: 1 <= DEVICE_COUNT
DEVICE_COUNT=${DEVICE_COUNT:-1}
# Note: tolerable_faults が 0 になるのを避けつつ、フラグメントが各サーバーに1つずつ
# 配置されるようにデフォルト値を決める。
TOLERABLE_FAULTS=${TOLERABLE_FAULTS:-$(( ($SERVER_COUNT / 2 - 1) >= 1 ? ($SERVER_COUNT / 2 - 1) : 1 ))}
# Note: $SERVER_COUNT + 1 は contact-server の分を $SERVER_COUNT に追加している。
DATA_FRAGMENTS=${DATA_FRAGMENTS:-$(( ($SERVER_COUNT + 1 - $TOLERABLE_FAULTS) >= 1 ? ($SERVER_COUNT + 1 - $TOLERABLE_FAULTS) : 1 ))}
FRUGALOS_START_FLAGS=${FRUGALOS_START_FLAGS:- --sampling-rate 1.0}

##
## Move exec to workdir
##
cp target/${MODE}/frugalos $WORK_DIR/bin/

##
## Creates window and panes
##
tmux kill-window -t $WIN || echo "OK"
tmux new-window -n $WIN -c $WORK_DIR

##
## Starts frugalos cluster
##
tmux send-keys -t $WIN.0 "bin/frugalos create --id srv0 --addr 127.0.0.1:${RPC_PORT} --data-dir srv0" C-m
tmux send-keys -t $WIN.0 "FRUGALOS_REPAIR_ENABLED=1 FRUGALOS_SNAPSHOT_THRESHOLD=10 bin/frugalos start --data-dir srv0 ${FRUGALOS_START_FLAGS} --http-server-bind-addr 127.0.0.1:${PORT}" C-m
sleep 6

for s in $(seq 1 ${SERVER_COUNT})
do
    server="srv${s}"
    http=$((${PORT} + ${s}))
    rpc=$((${RPC_PORT} + ${s}))
    tmux kill-window -t ${server} || echo "OK: ${server}"
    tmux new-window -d -n "${server}.0" -c ${WORK_DIR}
    tmux send-keys -t ${server}.0 "bin/frugalos join --id ${server} --addr 127.0.0.1:${rpc} --data-dir ${server} --contact-server 127.0.0.1:${RPC_PORT}" C-m
    sleep 1
    tmux send-keys -t ${server}.0 "FRUGALOS_REPAIR_ENABLED=1 FRUGALOS_SNAPSHOT_THRESHOLD=10 bin/frugalos start --data-dir ${server} ${FRUGALOS_START_FLAGS} --http-server-bind-addr 127.0.0.1:${http}" C-m
    sleep 3
done
