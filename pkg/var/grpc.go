package _var

import (
	"sync"
)

type GrpcConfigItem struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

var grpcList = make(map[string]*GrpcConfigItem)
var grpcLock = &sync.Mutex{}

func ListAllGrpcHost() map[string]*GrpcConfigItem {
	if grpcList != nil && len(grpcList) > 0 {
		return grpcList
	}

	grpcLock.Lock()
	defer grpcLock.Unlock()

	if grpcList != nil && len(grpcList) > 0 {
		return grpcList
	}

	return ListAllConfig()
}

func ListAllConfig() map[string]*GrpcConfigItem {
	grpcList["push"] = &GrpcConfigItem{
		Name:    "push",
		Address: "0.0.0.0:50051",
	}

	return grpcList
}

func GetGrpcHostByServiceName(name string) *GrpcConfigItem {
	all := ListAllGrpcHost()
	if all == nil || len(all) == 0 {
		return nil
	}

	if item, ok := all[name]; ok {
		return item
	}

	return nil
}
