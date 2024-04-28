package grpc

import (
	"context"
	"errors"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/log"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/pool"
	_var "github.com/garfieldlw/kafka-delay-queue-golang/pkg/var"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"sync"
	"time"
)

var (
	initNum      = 64
	initIdle     = 64
	initCapacity = 1024

	grpcServiceMap = make(map[string]pool.Pool)
	lock           = &sync.Mutex{}
)

type ServiceGrpcPoolConfig struct {
	ServiceName string
	Address     string
	init        int
	idle        int
	capacity    int
	idleTimeout time.Duration
}

func WithClientInterceptor() grpc.DialOption {
	return grpc.WithUnaryInterceptor(clientInterceptor())
}

func WithStreamInterceptor() grpc.DialOption {
	return grpc.WithStreamInterceptor(streamClientInterceptor())
}

func WithKeepaliveParams() grpc.DialOption {
	return grpc.WithKeepaliveParams(
		keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 3,
			PermitWithoutStream: true,
		})
}

func NewServiceGrpcConfig(name, address string, idle time.Duration) *ServiceGrpcPoolConfig {
	return &ServiceGrpcPoolConfig{
		ServiceName: name,
		Address:     address,
		init:        initNum,
		idle:        initIdle,
		capacity:    initCapacity,
		idleTimeout: idle,
	}
}

func clientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, resp interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()

		err := invoker(ctx, method, req, resp, cc, opts...)
		if err != nil {
			log.Error("Invoked RPC Error[Client]", zap.String("method", method), zap.Error(err))
		}

		log.Info("Invoked RPC[Client]", zap.String("method", method), zap.String("Duration", time.Since(start).String()), zap.Error(err))
		return err
	}
}

func streamClientInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		start := time.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)

		if err != nil {
			log.Error("Invoked RPC Error[Stream]", zap.String("method", method), zap.Error(err))
		}

		log.Info("Invoked RPC[Stream]", zap.String("method", method), zap.String("Duration", time.Since(start).String()), zap.Error(err))

		return cs, err
	}
}

func CreatePoll(config *ServiceGrpcPoolConfig, opts ...grpc.DialOption) (pool.Pool, error) {
	if opts == nil {
		opts = []grpc.DialOption{}
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()), WithClientInterceptor(), WithStreamInterceptor(), WithKeepaliveParams())
	return pool.NewChannelPool(&pool.Config{
		Factory: func() (interface{}, error) {
			return grpc.Dial(config.Address, opts...)
		},
		InitialCap: config.init,
		MaxIdle:    config.idle,
		MaxCap:     config.capacity,
		Close: func(i interface{}) error {
			if v, ok := i.(*grpc.ClientConn); ok {
				return v.Close()
			}
			return nil
		},
		//连接最大空闲时间，超过该时间的连接 将会关闭，可避免空闲时连接EOF，自动失效的问题
		IdleTimeout: config.idleTimeout,
		//Ping: func(i interface{}) error {
		//	if v, ok := i.(*grpc.ClientConn); ok {
		//		if v.GetState() == connectivity.Connecting || v.GetState() == connectivity.Ready || v.GetState() == connectivity.Idle {
		//			return nil
		//		}
		//	}
		//	return errors.New("connect closed")
		//},
	})
}

func LoadServicePool(serviceName string) (pool.Pool, error) {
	if v, ok := grpcServiceMap[serviceName]; ok {
		if p, valid := v.(pool.Pool); valid {
			return p, nil
		} else {
			delete(grpcServiceMap, serviceName)
		}
	}

	lock.Lock()
	defer lock.Unlock()

	if v, ok := grpcServiceMap[serviceName]; ok {
		if p, valid := v.(pool.Pool); valid {
			return p, nil
		} else {
			delete(grpcServiceMap, serviceName)
		}
	}

	value := _var.GetGrpcHostByServiceName(serviceName)
	if value == nil {
		log.Warn("get grpc config error")
		return nil, errors.New("get grpc config error")
	}

	p, err := CreatePoll(NewServiceGrpcConfig(serviceName, value.Address, time.Second))
	if err != nil {
		log.Warn("get pool error", zap.Error(err))
		return nil, err
	}

	grpcServiceMap[serviceName] = p

	return grpcServiceMap[serviceName], nil
}
