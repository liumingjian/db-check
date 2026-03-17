package web

type wsLogMessage struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

type wsProgressMessage struct {
	Type        string `json:"type"`
	Completed   int    `json:"completed"`
	Total       int    `json:"total"`
	CurrentFile string `json:"current_file"`
}

type wsDoneMessage struct {
	Type        string `json:"type"`
	DownloadURL string `json:"download_url"`
}

type wsErrorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
