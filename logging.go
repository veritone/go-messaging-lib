package messaging

import (
	"log"
	"os"
	"strings"

	"go.uber.org/zap"
)

type Logger = *zap.Logger

func AddLogger(env string) (Logger, error) {
	if env == "" {
		env, _ = os.LookupEnv("LOGGER")
	}
	var (
		logger *zap.Logger
		err    error
	)
	switch strings.ToLower(env) {
	case "dev":
		logger, err = zap.NewDevelopment()
	default:
		logger, err = zap.NewProduction()
	}
	return logger, err
}

func MustAddLogger(env string) Logger {
	logger, err := AddLogger(env)
	if err != nil {
		// Panic with standard logging
		log.Panic(err)
	}
	return logger
}

func MapToLogger(data map[string]string) []zap.Field {
	fields := []zap.Field{}
	for k, v := range data {
		fields = append(fields, zap.String(k, v))
	}
	return fields
}
