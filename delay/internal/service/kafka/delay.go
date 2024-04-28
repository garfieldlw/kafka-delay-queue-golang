package kafka

import (
	"context"
	"fmt"
	"time"
)

const (
	second          = 1
	minute          = 60 * second
	hour            = 60 * minute
	sleepBaseSecond = 5

	delay1m  = 1 * minute
	delay3m  = 3 * minute
	delay10m = 10 * minute
	delay15m = 15 * minute
	delay30m = 30 * minute
)

func delayUntil(nowSecond, untilSecond int64) (runNow bool, err error) {
	if diff := untilSecond - nowSecond; diff > 0 {
		nb, ns := cutSecond(diff)
		if err = sleepDuration(nb, time.Second*sleepBaseSecond); err != nil {
			return false, err
		}
		if err = sleepDuration(ns, time.Second); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func sleepDuration(n int64, duration time.Duration) (err error) {
	for i := int64(0); i < n; i++ {
		if err = exitSleep(); err != nil {
			return fmt.Errorf("sleep (%d/%d/%ds)：%v", i, n, duration/second, err)
		}
		time.Sleep(duration)
	}
	return nil
}

func cutSecond(ts int64) (nb int64, ns int64) {
	nb = ts / sleepBaseSecond
	ns = ts % sleepBaseSecond
	return
}

var (
	sleepCtx, sleepCancelFunc = context.WithCancel(context.Background())
)

func CancelDelaySleep() {
	sleepCancelFunc()
}

func exitSleep() error { // context cancel exit sleep
	return sleepCtx.Err()
}
