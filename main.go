package main

import (
	"context"
	"errors"
	"github.com/melgenek/f8n/pkg/app"
	"github.com/melgenek/f8n/pkg/drivers/fdb"
	_ "github.com/melgenek/f8n/pkg/drivers/fdb"
	"github.com/sirupsen/logrus"
	"os"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	a := app.New()
	a.Flags = append(a.Flags, fdb.ConfigFlags()...)
	if err := a.Run(os.Args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Fatal(err)
		}
	}
}
