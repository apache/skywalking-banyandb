package cmd_test

import (
	"bytes"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	"log"
	"os/exec"
)

var _ = Describe("Stream", func() {
	It("exec bydbctl without any parameter", func() {
		// todo: start banyand-server

		cmd := exec.Command("../../build/bin/bydbctl")
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s", out.String())
	})

	It("exec bydbctl stream list operation", func() {
		// todo: start banyand-server
		cmd := exec.Command("../../build/bin/bydbctl", "stream list", "-j \"{\\\"group\\\":\\\"mxm\\\",\\\"name\\\":\\\"naonao\\\"}\"")
		fmt.Println(cmd)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s", out.String())
	})
})
