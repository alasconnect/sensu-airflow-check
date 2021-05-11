package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sensu-community/sensu-plugin-sdk/sensu"
	"github.com/sensu/sensu-go/types"
)

// Config represents the check plugin config.
type Config struct {
	sensu.PluginConfig
	AirflowApiUrl   string
	AirflowUsername string
	AirflowPassword string
	Timeout         int
}

var (
	plugin = Config{
		PluginConfig: sensu.PluginConfig{
			Name:     "airflow-import-check",
			Short:    "Check the Airflow 2.x DAG import errors endpoint. Returns critical if there are errors.",
			Keyspace: "sensu.io/plugins/airflow-import-check/config",
		},
	}

	options = []*sensu.PluginConfigOption{
		{
			Path:      "airflow-api-url",
			Env:       "",
			Argument:  "url",
			Shorthand: "u",
			Default:   "http://127.0.0.1:8080/",
			Usage:     "The base URL of the airflow REST API.",
			Value:     &plugin.AirflowApiUrl,
		},
		{
			Path:      "airflow-username",
			Env:       "",
			Argument:  "username",
			Shorthand: "n",
			Default:   "",
			Usage:     "The username used to authenticate against the airflow API.",
			Value:     &plugin.AirflowUsername,
		},
		{
			Path:      "airflow-password",
			Env:       "",
			Argument:  "password",
			Shorthand: "p",
			Default:   "",
			Usage:     "The password used to authenticate against the airflow API.",
			Value:     &plugin.AirflowPassword,
		},
		{
			Path:      "timeout",
			Env:       "",
			Argument:  "timeout",
			Shorthand: "t",
			Default:   15,
			Usage:     "Request timeout in seconds",
			Value:     &plugin.Timeout,
		},
	}
)

func main() {
	check := sensu.NewGoCheck(&plugin.PluginConfig, options, checkArgs, executeCheck, false)
	check.Execute()
}

func checkArgs(event *types.Event) (int, error) {
	_, err := url.Parse(plugin.AirflowApiUrl)
	if err != nil {
		return sensu.CheckStateWarning, fmt.Errorf("failed to parse airflow URL %s: %v", plugin.AirflowApiUrl, err)
	}

	if plugin.AirflowUsername == "" {
		return sensu.CheckStateWarning, fmt.Errorf("airflow username is required")
	}

	if plugin.AirflowPassword == "" {
		return sensu.CheckStateWarning, fmt.Errorf("airflow password is required")
	}

	return sensu.CheckStateOK, nil
}

func executeCheck(event *types.Event) (int, error) {
	client := http.DefaultClient
	client.Transport = http.DefaultTransport
	client.Timeout = time.Duration(plugin.Timeout) * time.Second

	critical := false
	var err error

	var importErrors *ImportErrors
	importErrors, err = getImportErrors(client)

	if err != nil {
		fmt.Printf("Error occurred while checking airflow import errors:\n%v\n", err)
		critical = true
	} else if importErrors.TotalEntries > 0 {
		critical = true
		for _, ie := range importErrors.ImportErrors {
			fmt.Printf("Airflow encountered an error while importing DAG: %s\n%v\n", ie.FileName, ie.StackTrace)
		}
	}

	if critical {
		return sensu.CheckStateCritical, nil
	}
	return sensu.CheckStateOK, nil
}

type ImportError struct {
	TimeStamp  string `json:"timestamp"`
	FileName   string `json:"filename"`
	StackTrace string `json:"stack_trace"`
}

type ImportErrors struct {
	ImportErrors []ImportError `json:"import_errors"`
	TotalEntries int           `json:"total_entries"`
}

func getAirflowApiUrl() string {
	// a trailing slash will cause errors
	return strings.TrimSuffix(plugin.AirflowApiUrl, "/") + "/api/v1"
}

func getImportErrors(client *http.Client) (*ImportErrors, error) {
	req, err := http.NewRequest("GET", getAirflowApiUrl()+"/importErrors", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(plugin.AirflowUsername, plugin.AirflowPassword)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("import errors request returned an invalid status code: %s", resp.Status)
	}

	defer resp.Body.Close()

	var result ImportErrors
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode health response: %v", err)
	}

	return &result, nil
}
