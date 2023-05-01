package utils

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogging() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func LogAndQuitIfErrorExists(err error) {
	if err != nil {
		log.Fatal().Err(err).Send()
	}
}
