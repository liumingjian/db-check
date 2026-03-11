package core

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const totalRunSteps = 11

type eventDescriptor struct {
	step  int
	label string
}

var eventDescriptors = map[string]eventDescriptor{
	"run_started":          {step: 1, label: "Run started"},
	"run_dir_prepared":     {step: 2, label: "Run directory prepared"},
	"os_collect_started":   {step: 3, label: "OS collection started"},
	"os_collect_finished":  {step: 4, label: "OS collection finished"},
	"os_collect_failed":    {step: 4, label: "OS collection failed"},
	"os_collect_skipped":   {step: 4, label: "OS collection skipped"},
	"db_collect_started":   {step: 5, label: "DB collection started"},
	"db_collect_finished":  {step: 6, label: "DB collection finished"},
	"db_collect_failed":    {step: 6, label: "DB collection failed"},
	"db_collect_skipped":   {step: 6, label: "DB collection skipped"},
	"result_write_started": {step: 7, label: "Result write started"},
	"result_written":       {step: 8, label: "Result written"},
	"result_write_skipped": {step: 8, label: "Result write skipped"},
	"manifest_write_started": {
		step:  9,
		label: "Manifest write started",
	},
	"manifest_written":      {step: 10, label: "Manifest written"},
	"manifest_write_failed": {step: 10, label: "Manifest write failed"},
	"run_finished":          {step: 11, label: "Run finished"},
	"result_write_failed":   {step: 8, label: "Result write failed"},
}

type runLogger struct {
	logger *zap.Logger
	buffer *bytes.Buffer
	runID  string
}

func newRunLogger(runID string) *runLogger {
	buf := &bytes.Buffer{}
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		NameKey:          "logger",
		MessageKey:       "msg",
		ConsoleSeparator: " ",
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
		},
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.AddSync(buf),
		zap.InfoLevel,
	)
	logger := zap.New(core).Named("collector.runner")
	return &runLogger{logger: logger, buffer: buf, runID: runID}
}

func (l *runLogger) Info(event string, fields ...zap.Field) {
	l.logger.Info(composeLogEntry(event, l.runID, fields, nil))
}

func (l *runLogger) Error(event string, err error, fields ...zap.Field) {
	l.logger.Error(composeLogEntry(event, l.runID, fields, err))
}

func (l *runLogger) Sync() {
	_ = l.logger.Sync()
}

func (l *runLogger) String() string {
	return l.buffer.String()
}

func composeLogEntry(event string, runID string, fields []zap.Field, err error) string {
	descriptor, ok := eventDescriptors[event]
	message := event
	segments := make([]string, 0, len(fields)+4)
	if ok {
		message = descriptor.label
		segments = append(segments, fmt.Sprintf("step=%d/%d", descriptor.step, totalRunSteps))
	}
	segments = append(segments, fmt.Sprintf("event=%s", event))
	segments = append(segments, fmt.Sprintf("run_id=%s", runID))
	for i := range fields {
		if token, include := renderFieldToken(fields[i]); include {
			segments = append(segments, token)
		}
	}
	if err != nil {
		segments = append(segments, encodeKV("error", err.Error()))
	}
	return fmt.Sprintf("%s | %s", message, strings.Join(segments, " "))
}

func renderFieldToken(field zap.Field) (string, bool) {
	switch field.Type {
	case zapcore.StringType:
		return encodeKV(field.Key, field.String), true
	case zapcore.BoolType:
		return encodeKV(field.Key, strconv.FormatBool(field.Integer == 1)), true
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type:
		return encodeKV(field.Key, strconv.FormatInt(field.Integer, 10)), true
	case zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type, zapcore.UintptrType:
		return encodeKV(field.Key, strconv.FormatUint(uint64(field.Integer), 10)), true
	case zapcore.Float64Type:
		value := math.Float64frombits(uint64(field.Integer))
		return encodeKV(field.Key, strconv.FormatFloat(value, 'f', -1, 64)), true
	case zapcore.Float32Type:
		value := math.Float32frombits(uint32(field.Integer))
		return encodeKV(field.Key, strconv.FormatFloat(float64(value), 'f', -1, 32)), true
	case zapcore.DurationType:
		return encodeKV(field.Key, time.Duration(field.Integer).String()), true
	case zapcore.ErrorType:
		if value, ok := field.Interface.(error); ok && value != nil {
			return encodeKV(field.Key, value.Error()), true
		}
		return "", false
	default:
		if field.Interface == nil {
			return "", false
		}
		return encodeKV(field.Key, fmt.Sprintf("%v", field.Interface)), true
	}
}

func encodeKV(key string, value string) string {
	if strings.ContainsAny(value, " \t\"=") {
		return fmt.Sprintf("%s=%q", key, value)
	}
	return fmt.Sprintf("%s=%s", key, value)
}
