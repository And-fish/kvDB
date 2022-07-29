package utils

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
func AssertTruef(b bool, fmt string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(fmt, args...))
	}
}
func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// Err err
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}
