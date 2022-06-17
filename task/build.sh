SCRIPT_PATH=$(dirname "$0")

protoc -I=$SCRIPT_PATH -I=$GOPATH/src --go_out=. --go_opt=paths=source_relative $SCRIPT_PATH/scheduler.proto
protoc -I=$SCRIPT_PATH -I=$GOPATH/src --gograinv2_out=. $SCRIPT_PATH/scheduler.proto

goreturns -w .
