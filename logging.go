package messaging

import (
	"log"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger = *zap.Logger

func AddLogger(level string) (Logger, error) {
	env, _ := os.LookupEnv("LOGGER")
	logLevel, found := supportedLevels[level]
	if !found {
		log.Printf("unsupported logLevel (%s). Defaulting to info. The supported levels are: error, warn, info, and debug", level)
		logLevel = zapcore.InfoLevel
	}
	var (
		logger *zap.Logger
		err    error
	)
	switch strings.ToLower(env) {
	case "dev":
		// Development puts the logger in development mode, which changes the
		// behavior of DPanicLevel and takes stacktraces more liberally.
		config := zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(logLevel)
		logger, err = config.Build()
	default:
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(logLevel)
		logger, err = config.Build()
	}
	return logger, err
}

func MustAddLogger(level string) Logger {
	logger, err := AddLogger(level)
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

var supportedLevels = map[string]zapcore.Level{
	"error": zapcore.ErrorLevel,
	"warn":  zapcore.WarnLevel,
	"info":  zapcore.InfoLevel,
	"debug": zapcore.DebugLevel,
}
