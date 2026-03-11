package cli

import (
	"fmt"
	"strconv"
	"strings"
)

func parseArgsIntoConfig(args []string, cfg *Config) (parsedState, error) {
	state := parsedState{}
	for index := 0; index < len(args); index++ {
		arg := args[index]
		value, hasValue := splitLongForm(arg)
		normalized := normalizeArg(arg, hasValue)
		next, err := applyArg(cfg, &state, args, index, normalized, value, hasValue)
		if err != nil {
			return state, err
		}
		index = next
	}
	return state, nil
}

func normalizeArg(arg string, hasValue bool) string {
	if !hasValue {
		return arg
	}
	return arg[:strings.Index(arg, "=")]
}

func applyArg(cfg *Config, state *parsedState, args []string, index int, arg string, value string, hasValue bool) (int, error) {
	if handled, next, err := applyMainArg(cfg, args, index, arg, value, hasValue); handled {
		return next, err
	}
	if handled, next, err := applySSHArg(cfg, state, args, index, arg, value, hasValue); handled {
		return next, err
	}
	if handled, next, err := applyOSCollectArg(cfg, state, args, index, arg, value, hasValue); handled {
		return next, err
	}
	return index, fmt.Errorf("未知参数: %s", args[index])
}

func applyMainArg(cfg *Config, args []string, index int, arg string, value string, hasValue bool) (bool, int, error) {
	if handled, next, err := applyDBArg(cfg, args, index, arg, value, hasValue); handled {
		return true, next, err
	}
	if handled, next, err := applyModeArg(cfg, index, arg); handled {
		return true, next, err
	}
	if handled, next, err := applyOutputArg(cfg, args, index, arg, value, hasValue); handled {
		return true, next, err
	}
	if handled, next, err := applyMetaArg(cfg, index, arg); handled {
		return true, next, err
	}
	return false, index, nil
}

func applyDBArg(cfg *Config, args []string, index int, arg string, value string, hasValue bool) (bool, int, error) {
	switch arg {
	case "--db-type", "-t":
		next, err := setStringValue(args, index, value, hasValue, &cfg.DBType)
		return true, next, err
	case "--db-host", "-h":
		next, err := setStringValue(args, index, value, hasValue, &cfg.DBHost)
		return true, next, err
	case "--db-port", "-P":
		next, err := setIntValue(args, index, value, hasValue, &cfg.DBPort)
		return true, next, err
	case "--db-username", "-u":
		next, err := setStringValue(args, index, value, hasValue, &cfg.DBUsername)
		return true, next, err
	case "--db-password", "-p":
		next, err := setStringValue(args, index, value, hasValue, &cfg.DBPassword)
		return true, next, err
	case "--dbname", "-d":
		next, err := setStringValue(args, index, value, hasValue, &cfg.DBName)
		return true, next, err
	default:
		return false, index, nil
	}
}

func applyModeArg(cfg *Config, index int, arg string) (bool, int, error) {
	switch arg {
	case "--local":
		cfg.Local = true
		return true, index, nil
	case "--os-only":
		cfg.OSOnly = true
		return true, index, nil
	case "--os-skip":
		cfg.OSSkip = true
		return true, index, nil
	default:
		return false, index, nil
	}
}

func applyOutputArg(cfg *Config, args []string, index int, arg string, value string, hasValue bool) (bool, int, error) {
	switch arg {
	case "--output-dir", "-o":
		next, err := setStringValue(args, index, value, hasValue, &cfg.OutputDir)
		return true, next, err
	case "--log-path":
		next, err := setStringValue(args, index, value, hasValue, &cfg.LogPath)
		return true, next, err
	case "--log-level":
		next, err := setStringValue(args, index, value, hasValue, &cfg.LogLevel)
		return true, next, err
	case "--sql-timeout":
		next, err := setIntValue(args, index, value, hasValue, &cfg.SQLTimeoutSeconds)
		return true, next, err
	case "--top-n", "-n":
		next, err := setIntValue(args, index, value, hasValue, &cfg.TopN)
		return true, next, err
	default:
		return false, index, nil
	}
}

func applyMetaArg(cfg *Config, index int, arg string) (bool, int, error) {
	switch arg {
	case "--version", "-v":
		cfg.ShowVersion = true
		return true, index, nil
	case "--help":
		cfg.ShowHelp = true
		return true, index, nil
	default:
		return false, index, nil
	}
}

func applySSHArg(cfg *Config, state *parsedState, args []string, index int, arg string, value string, hasValue bool) (bool, int, error) {
	switch arg {
	case "--os-host":
		state.SSHFlagsProvided = true
		next, err := setStringValue(args, index, value, hasValue, &cfg.OSHost)
		return true, next, err
	case "--os-port":
		state.SSHFlagsProvided = true
		next, err := setIntValue(args, index, value, hasValue, &cfg.OSPort)
		return true, next, err
	case "--os-username":
		state.SSHFlagsProvided = true
		next, err := setStringValue(args, index, value, hasValue, &cfg.OSUsername)
		return true, next, err
	case "--os-password":
		state.SSHFlagsProvided = true
		next, err := setStringValue(args, index, value, hasValue, &cfg.OSPassword)
		return true, next, err
	case "--os-ssh-key-path":
		state.SSHFlagsProvided = true
		next, err := setStringValue(args, index, value, hasValue, &cfg.OSSSHKeyPath)
		return true, next, err
	case "--remote-collector-path":
		state.SSHFlagsProvided = true
		next, err := setStringValue(args, index, value, hasValue, &cfg.RemoteCollectorPath)
		return true, next, err
	default:
		return false, index, nil
	}
}

func applyOSCollectArg(cfg *Config, state *parsedState, args []string, index int, arg string, value string, hasValue bool) (bool, int, error) {
	switch arg {
	case "--os-collect-interval":
		state.IntervalChanged = true
		next, err := setIntValue(args, index, value, hasValue, &cfg.OSCollectInterval)
		return true, next, err
	case "--os-collect-duration":
		state.DurationChanged = true
		next, err := setIntValue(args, index, value, hasValue, &cfg.OSCollectDuration)
		return true, next, err
	case "--os-collect-count":
		state.CountChanged = true
		next, err := setIntValue(args, index, value, hasValue, &cfg.OSCollectCount)
		return true, next, err
	default:
		return false, index, nil
	}
}

func splitLongForm(arg string) (string, bool) {
	if !strings.HasPrefix(arg, "--") {
		return "", false
	}
	index := strings.Index(arg, "=")
	if index < 0 {
		return "", false
	}
	return arg[index+1:], true
}

func setStringValue(args []string, index int, inline string, hasInline bool, target *string) (int, error) {
	value, next, err := readStringValue(args, index, inline, hasInline)
	if err != nil {
		return index, err
	}
	*target = value
	return next, nil
}

func setIntValue(args []string, index int, inline string, hasInline bool, target *int) (int, error) {
	value, next, err := readIntValue(args, index, inline, hasInline)
	if err != nil {
		return index, err
	}
	*target = value
	return next, nil
}

func readStringValue(args []string, index int, inline string, hasInline bool) (string, int, error) {
	if hasInline {
		return inline, index, nil
	}
	next := index + 1
	if next >= len(args) {
		return "", index, fmt.Errorf("参数缺少值: %s", args[index])
	}
	return args[next], next, nil
}

func readIntValue(args []string, index int, inline string, hasInline bool) (int, int, error) {
	value, next, err := readStringValue(args, index, inline, hasInline)
	if err != nil {
		return 0, index, err
	}
	parsed, parseErr := strconv.Atoi(value)
	if parseErr != nil {
		return 0, index, fmt.Errorf("参数值必须为整数: %s", args[index])
	}
	return parsed, next, nil
}
