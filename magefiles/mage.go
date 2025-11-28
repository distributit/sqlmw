//go:build mage

package main

import (
	"context"

	"github.com/magefile/mage/sh"
)

// Lint runs the linter
func Lint(ctx context.Context) error {
	//args := []string{"tool", "-modfile=./.tools/go.mod", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint", "run", "--config", ".golangci.yml"}
	args := []string{"run", "--config", ".golangci.yml"}
	return sh.RunV("golangci-lint", args...)
}

// Test runs the tests
func Test(ctx context.Context) error {
	args := []string{"test", "-v", "-race", "./..."}
	return sh.RunV("go", args...)
}
