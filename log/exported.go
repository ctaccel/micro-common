package log

import (
	"fmt"
	"log/syslog"
	"net/http"
	"os"

	"github.com/tchap/zapext/zapsyslog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	encoderConfig = zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "linenum",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}

	logger = zap.L()
)

// SetEncoderConfig set log config
func SetEncoderConfig(config zapcore.EncoderConfig) {
	encoderConfig = config
}

// NewConsoleCore create a console core
func NewConsoleCore(level zap.AtomicLevel) zapcore.Core {
	// 设置日志级别
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.Lock(os.Stdout),
		level,
	)
}

func NewFileCore(level zap.AtomicLevel, filePath string, maxSize, maxBackups, maxAge int, compress bool) zapcore.Core {
	hook := lumberjack.Logger{
		Filename:   filePath,   // 日志文件路径
		MaxSize:    maxSize,    // 每个日志文件保存的最大尺寸 单位：M
		MaxBackups: maxBackups, // 日志文件最多保存多少个备份
		MaxAge:     maxAge,     // 文件最多保存多少天
		Compress:   compress,   // 是否压缩
	}
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(&hook),
		level,
	)
}

// NewSyslogCore creat a syslog zap core
func NewSyslogCore(level zap.AtomicLevel, network, raddr string, priority syslog.Priority, tag string) (*zapsyslog.Core, error) {
	writer, err := syslog.Dial(network, raddr, priority, tag)
	if err != nil {
		return nil, fmt.Errorf("Failed to set up syslog: %s", err.Error())
	}

	// Initialize Zap.
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	return zapsyslog.NewCore(level, encoder, writer), nil
}

// WatchLogLevel listen on http, and change level online
func WatchLogLevel(addr string) zap.AtomicLevel {
	alevel := zap.NewAtomicLevel()
	http.HandleFunc("/handle/level", alevel.ServeHTTP)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			panic(err)
		}
	}()

	return alevel
}

// SetGlobalLogger replace global log
func SetGlobalLogger(cores ...zapcore.Core) {
	core := zapcore.NewTee(cores...)
	zap.ReplaceGlobals(zap.New(core))

	logger = zap.L()
}

// With creates a child logger and adds structured context to it. Fields added
// to the child don't affect the parent, and vice versa.
func With(fields ...zap.Field) *zap.Logger {
	return logger.With(fields...)
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

// DPanic logs a message at DPanicLevel. The message includes any fields
// passed at the log site, as well as any fields accumulated on the logger.
//
// If the logger is in development mode, it then panics (DPanic means
// "development panic"). This is useful for catching errors that are
// recoverable, but shouldn't ever happen.
func DPanic(msg string, fields ...zap.Field) {
	logger.DPanic(msg, fields...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zap.Field) {
	logger.Panic(msg, fields...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func Fatal(msg string, fields ...zap.Field) {
	logger.Fatal(msg, fields...)
}

// Sync calls the underlying Core's Sync method, flushing any buffered log
// entries. Applications should take care to call Sync before exiting.
func Sync() error {
	return logger.Sync()
}
