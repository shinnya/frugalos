#! /bin/bash

set -eux

source $(cd $(dirname $0); pwd)/common.sh

FRUGALOS_BIN=$(dirname $0)/../../target/debug/frugalos 

run_command() {
    sudo rm -rf /tmp/frugalos_it/
    $FRUGALOS_BIN $@
}

# loglevel
for LOGLEVEL in debug info warning error critical;
do
    run_command --loglevel $LOGLEVEL create --id srv5 --data-dir $WORK_DIR/srv1
done

# max concurent logs
for MAX_CONCURRENTLOGS in 64 4096 10000;
do
    run_command --max_concurrent_logs $MAX_CONCURRENTLOGS create --id srv5 --data-dir $WORK_DIR/srv1
done
