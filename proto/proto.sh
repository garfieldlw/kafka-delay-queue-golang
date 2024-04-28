find ./ | grep '\.proto' | xargs -n1 protoc  --go-grpc_out=require_unimplemented_servers=false:. --go_out=.
