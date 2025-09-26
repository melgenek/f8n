package fdb

import (
	"bufio"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func newestTrace() (string, error) {
	files, _ := filepath.Glob("/tmp/trace*.json")
	if len(files) == 0 {
		return "", fmt.Errorf("no files found")
	}
	sort.Slice(files, func(i, j int) bool {
		fi, _ := os.Stat(files[i])
		fj, _ := os.Stat(files[j])
		return fi.ModTime().After(fj.ModTime())
	})
	return files[0], nil
}

func followFile(file string) {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	r := bufio.NewReader(f)

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		logrus.Debug(strings.TrimRight(line, "\n"))
	}
}

func logFDBTraces() error {
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		if err := fdb.Options().SetTraceEnable("/tmp"); err != nil {
			return err
		}
		if err := fdb.Options().SetTraceFormat("json"); err != nil {
			return err
		}
		go func() {
			prevFile := ""
			for {
				nextFile, err := newestTrace()
				if err == nil && nextFile != prevFile {
					followFile(nextFile)
					prevFile = nextFile
				}
			}
		}()
	}
	return nil
}
