package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev2 "github.com/sensu/core/v2"
	"github.com/sensu/sensu-plugin-sdk/sensu"
)

// Config represents the check plugin config.
type Config struct {
	sensu.PluginConfig
	AirflowApiUrl string
	Timeout       int
}

var (
	plugin = Config{
		PluginConfig: sensu.PluginConfig{
			Name:     "airflow-check",
			Short:    "Check the health endpoint of Airflow 2.x. Returns critical if not healthy.",
			Keyspace: "sensu.io/plugins/airflow-check/config",
		},
	}

	options = []sensu.ConfigOption{
		&sensu.PluginConfigOption[string]{
			Path:      "airflow-api-url",
			Env:       "",
			Argument:  "url",
			Shorthand: "u",
			Default:   "http://127.0.0.1:8080/",
			Usage:     "The base URL of the airflow REST API.",
			Value:     &plugin.AirflowApiUrl,
		},
		&sensu.PluginConfigOption[int]{
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

func checkArgs(event *corev2.Event) (int, error) {
	_, err := url.Parse(plugin.AirflowApiUrl)
	if err != nil {
		return sensu.CheckStateWarning, fmt.Errorf("failed to parse airflow URL %s: %v", plugin.AirflowApiUrl, err)
	}

	return sensu.CheckStateOK, nil
}

func executeCheck(event *corev2.Event) (int, error) {
	client := http.DefaultClient
	client.Transport = http.DefaultTransport
	client.Timeout = time.Duration(plugin.Timeout) * time.Second

	critical := false
	var err error

	var health *Health
	health, err = getHealth(client)

	if err != nil {
		fmt.Printf("Error occurred while checking airflow health:\n%v\n", err)
		critical = true
	} else {
		if health.MetaDatabaseHealth.Status != "healthy" {
			fmt.Printf("Airflow metadatabase is in trouble.")
			critical = true
		}

		if health.Scheduler.Status != "healthy" {
			fmt.Printf("Airflow scheduler is in trouble.")
			critical = true
		}
	}

	if critical {
		return sensu.CheckStateCritical, nil
	}
	return sensu.CheckStateOK, nil
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

func getAirflowApiUrl() string {
	// a trailing slash will cause errors
	return strings.TrimSuffix(plugin.AirflowApiUrl, "/") + "/api/v1"
}

func getHealth(client *http.Client) (*Health, error) {
	req, err := http.NewRequest("GET", getAirflowApiUrl()+"/health", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")

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
