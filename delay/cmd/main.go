package main

import (
	"github.com/garfieldlw/kafka-delay-queue-golang/delay/internal/service/kafka"
	"github.com/garfieldlw/kafka-delay-queue-golang/pkg/log"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Info("kafka sub start")

	startHandler()

	var stopSign = make(chan error)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	go func() {
		for {
			s := <-c
			log.Info("get a signal", zap.String("signal", s.String()))
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL:
				kafka.Close()
				stopSign <- nil
				return
			case syscall.SIGHUP:
			default:
				return
			}
		}
	}()

	<-stopSign
}

func startHandler() {
	err := kafka.RunSub()
	if err != nil {
		log.Fatal("start kafka fatal error", zap.Error(err))
	}
}
