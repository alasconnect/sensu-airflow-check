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
			Name:     "sensu-airflow-check",
			Short:    "A plugin for checking the health of airflow 2.0 in Sensu.",
			Keyspace: "sensu.io/plugins/airflow-check/config",
		},
	}

	options = []*sensu.PluginConfigOption{
		{
			Path:      "airflow-api-url",
			Env:       "",
			Argument:  "url",
			Shorthand: "u",
			Default:   "https://127.0.0.1:8081/",
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
		return sensu.CheckStateWarning, fmt.Errorf("Airflow username is required")
	}

	if plugin.AirflowPassword == "" {
		return sensu.CheckStateWarning, fmt.Errorf("Airflow password is required")
	}

	return sensu.CheckStateOK, nil
}

func executeCheck(event *types.Event) (int, error) {
	client := http.DefaultClient
	client.Transport = http.DefaultTransport
	client.Timeout = time.Duration(plugin.Timeout) * time.Second

	criticals := 0

	var err error
	var importErrors *ImportErrors
	importErrors, err = getImportErrors(client)

	if err != nil {
		fmt.Printf("Error occurred while checking airflow import errors:\n%v\n", err)
		criticals++
	} else if importErrors.TotalEntries > 0 {
		criticals++
		for _, ie := range importErrors.ImportErrors {
			fmt.Printf("Airflow encountered an error while importing dag: %s\n%v\n", ie.FileName, ie.StackTrace)
		}
	}

	var health *Health
	health, err = getHealth(client)

	if err != nil {
		fmt.Printf("Error occurred while checking airflow health:\n%v\n", err)
		criticals++
	} else {
		if health.MetaDatabaseHealth.Status != "healthy" {
			fmt.Printf("Airflow metadatabase is in trouble.")
			criticals++
		}

		if health.Scheduler.Status != "healthy" {
			fmt.Printf("Airflow scheduler is in trouble.")
			criticals++
		}
	}

	result := sensu.CheckStateOK
	if criticals > 0 {
		result = sensu.CheckStateCritical
	}
	return result, nil
}

type MetaDatabaseHealth struct {
	Status string `json:"status"`
}

type SchedulerHealth struct {
	Status                   string `json:"status"`
	LatestSchedulerHeartbeat string `json:"latest_scheduler_heartbeat"`
}

type Health struct {
	MetaDatabaseHealth MetaDatabaseHealth `json:"metadatabase"`
	Scheduler          SchedulerHealth    `json:"scheduler"`
}

func getHealth(client *http.Client) (*Health, error) {
	req, err := http.NewRequest("GET", getAirflowApiUrl()+"/health", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(plugin.AirflowUsername, plugin.AirflowPassword)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("health request returned an invalid status code: %s", resp.Status)
	}

	defer resp.Body.Close()

	var result Health
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode health response: %v", err)
	}

	return &result, nil
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

func getAirflowApiUrl() string {
	// a trailing slash will cause errors
	return strings.TrimSuffix(plugin.AirflowApiUrl, "/") + "/api/v1"
}
