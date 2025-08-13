package main

import (
	"context"
	"errors"
	kine "github.com/k3s-io/kine/pkg/app"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/melgenek/f8n/pkg/drivers/fdb"
	"github.com/sirupsen/logrus"
	"os"
)

func init() {
	drivers.Register("fdb", fdb.New)
}

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	app := kine.New()
	if err := app.Run(os.Args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Fatal(err)
		}
	}
}
