// Command noop is the entrypoint the pull pod runs inside every pre-pulled image.
//
// With no arguments it exits 0, so a container that uses any image as its root
// filesystem finishes as soon as the kubelet has pulled that image. With
// "install <path>" it copies itself to <path>, which is how the binary reaches
// the shared volume those containers mount.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	if len(os.Args) == 1 {
		return
	}
	if len(os.Args) != 3 || os.Args[1] != "install" {
		fmt.Fprintln(os.Stderr, "usage: noop [install <path>]")
		os.Exit(2)
	}
	if err := install(os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func install(dest string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	src, err := os.Open(self)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o555)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}
