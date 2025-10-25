package main

import (
	"flag"
	"net/http"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/yevhenii/myproj/f8n/benchmark/fdb-exporter/metrics"
)

var (
	clusterFile   = flag.String("cluster-file", "/etc/foundationdb/fdb.cluster", "Path to the FDB cluster file")
	listenAddress = flag.String("listen-address", ":9444", "Address to listen on for telemetry")
	metricsPath   = flag.String("metrics-path", "/metrics", "Path to expose metrics on")
	logLevel      = flag.String("log-level", "info", "Log level")
)

func main() {
	flag.Parse()

	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to parse log level")
	}
	logrus.SetLevel(level)

	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDatabase(*clusterFile)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to open FDB database")
	}

	exporter := metrics.NewExporter(db)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())

	logrus.WithFields(logrus.Fields{
		"listen_address": *listenAddress,
		"metrics_path":   *metricsPath,
	}).Info("Starting FDB exporter")
	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		logrus.WithError(err).Fatal("Failed to start HTTP server")
	}
}
