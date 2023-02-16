//go:build tools
// +build tools

// This file is used to ensure that go.mod has the proper config to run tests in CI.
// See: https://onsi.github.io/ginkgo/#recommended-continuous-integration-configuration

package main

import (
	_ "github.com/onsi/ginkgo/v2/ginkgo"
)
