package main

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionFormat(t *testing.T) {
	t.Run("the version string should be semver compatible", func(t *testing.T) {
		// from: https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
		semver := regexp.MustCompile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$")
		assert.True(t, semver.MatchString(service_version))
	})
}
