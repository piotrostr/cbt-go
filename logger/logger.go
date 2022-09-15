package logger

import (
	"log"

	"go.uber.org/zap"
)

func NewLogger() *zap.Logger {
	opts := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1),
	}
	logger, err := zap.NewProduction(opts...)
	if err != nil {
		log.Fatalf("zap.NewProduction error: %v\n", err)
	}
	return logger
}
