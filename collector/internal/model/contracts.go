package model

import "time"

type ModuleStat struct {
	Status     string  `json:"status"`
	DurationMS int64   `json:"duration_ms"`
	Error      *string `json:"error"`
}

type Artifacts struct {
	Log     string  `json:"log"`
	Result  *string `json:"result"`
	Summary *string `json:"summary"`
	Report  *string `json:"report"`
}

type Manifest struct {
	SchemaVersion string                `json:"schema_version"`
	RunID         string                `json:"run_id"`
	DBType        string                `json:"db_type"`
	StartTime     string                `json:"start_time"`
	EndTime       string                `json:"end_time"`
	ExitCode      int                   `json:"exit_code"`
	OverallStatus string                `json:"overall_status"`
	ModuleStats   map[string]ModuleStat `json:"module_stats"`
	Artifacts     Artifacts             `json:"artifacts"`
}

type ResultMeta struct {
	SchemaVersion    string `json:"schema_version"`
	CollectorVersion string `json:"collector_version"`
	DBType           string `json:"db_type"`
	DBHost           string `json:"db_host"`
	DBPort           int    `json:"db_port"`
	DBName           string `json:"db_name"`
	Timezone         string `json:"timezone"`
	CollectTime      string `json:"collect_time"`
}

type CollectConfig struct {
	SampleMode            string `json:"sample_mode"`
	SampleIntervalSeconds *int   `json:"sample_interval_seconds"`
	SamplePeriodSeconds   *int   `json:"sample_period_seconds"`
	ExpectedSamples       int    `json:"expected_samples"`
}

type CollectWindow struct {
	WindowStart     string `json:"window_start"`
	WindowEnd       string `json:"window_end"`
	DurationSeconds int    `json:"duration_seconds"`
}

type Result struct {
	Meta          ResultMeta     `json:"meta"`
	CollectConfig CollectConfig  `json:"collect_config"`
	CollectWindow CollectWindow  `json:"collect_window"`
	OS            map[string]any `json:"os"`
	DB            map[string]any `json:"db"`
}

type RunArtifacts struct {
	RunID        string
	RunDir       string
	ManifestPath string
	ResultPath   string
	LogPath      string
	Manifest     Manifest
	Result       *Result
}

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}
