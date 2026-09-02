package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type job struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	WorkloadType string `json:"workload_type"`
}

type result struct {
	job     job
	latency time.Duration
	err     error
}

type summary struct {
	Submitted       int            `json:"submitted"`
	SubmissionError int            `json:"submission_errors"`
	Succeeded       int            `json:"succeeded"`
	Failed          int            `json:"failed"`
	Unfinished      int            `json:"unfinished"`
	WorkloadRoutes  map[string]int `json:"workload_routes"`
	SubmissionP50MS float64        `json:"submission_p50_ms"`
	SubmissionP95MS float64        `json:"submission_p95_ms"`
	SubmissionP99MS float64        `json:"submission_p99_ms"`
	ElapsedSeconds  float64        `json:"elapsed_seconds"`
}

func main() {
	var (
		baseURL     = flag.String("url", "http://localhost:8080", "TaskGrid API base URL")
		token       = flag.String("token", defaultToken(), "TaskGrid admin or submit token")
		total       = flag.Int("jobs", 120, "number of jobs to submit")
		concurrency = flag.Int("concurrency", 20, "concurrent submissions")
		wait        = flag.Bool("wait", true, "wait for every submitted job to finish")
		timeout     = flag.Duration("timeout", 5*time.Minute, "overall benchmark timeout")
	)
	flag.Parse()

	if *total < 1 || *concurrency < 1 {
		fatal("jobs and concurrency must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	started := time.Now()
	client := &http.Client{Timeout: 15 * time.Second}
	runID := fmt.Sprintf("load-%d", started.UTC().UnixNano())

	results := submitAll(ctx, client, *baseURL, *token, runID, *total, *concurrency)
	report := summary{WorkloadRoutes: map[string]int{}}
	latencies := make([]time.Duration, 0, len(results))
	jobs := make([]job, 0, len(results))
	for _, item := range results {
		if item.err != nil {
			report.SubmissionError++
			fmt.Fprintln(os.Stderr, item.err)
			continue
		}
		report.Submitted++
		report.WorkloadRoutes[item.job.WorkloadType]++
		latencies = append(latencies, item.latency)
		jobs = append(jobs, item.job)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	report.SubmissionP50MS = percentileMS(latencies, 0.50)
	report.SubmissionP95MS = percentileMS(latencies, 0.95)
	report.SubmissionP99MS = percentileMS(latencies, 0.99)

	if *wait && len(jobs) > 0 {
		states := waitForJobs(ctx, client, *baseURL, *token, jobs, *concurrency)
		report.Succeeded = states["SUCCEEDED"]
		report.Failed = states["FAILED"]
		report.Unfinished = len(jobs) - report.Succeeded - report.Failed
	} else {
		report.Unfinished = len(jobs)
	}

	report.ElapsedSeconds = time.Since(started).Seconds()
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err.Error())
	}
	fmt.Println(string(output))
	if report.SubmissionError > 0 || report.Failed > 0 || (*wait && report.Unfinished > 0) {
		os.Exit(1)
	}
}

func defaultToken() string {
	if token := os.Getenv("TASKGRID_ADMIN_TOKEN"); token != "" {
		return token
	}
	return os.Getenv("TASKGRID_API_TOKEN")
}

func submitAll(ctx context.Context, client *http.Client, baseURL, token, runID string, total, concurrency int) []result {
	work := make(chan int)
	results := make(chan result, total)
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range work {
				results <- submitOne(ctx, client, baseURL, token, runID, index)
			}
		}()
	}
	go func() {
		defer close(work)
		for index := 0; index < total; index++ {
			select {
			case <-ctx.Done():
				return
			case work <- index:
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	collected := make([]result, 0, total)
	for item := range results {
		collected = append(collected, item)
	}
	return collected
}

func submitOne(ctx context.Context, client *http.Client, baseURL, token, runID string, index int) result {
	workload := []string{"cpu", "io", "general"}[index%3]
	description := map[string]string{
		"cpu":     "[workload:cpu] compute benchmark workload",
		"io":      "[workload:io] network wait workload",
		"general": "[workload:general] lightweight notification workload",
	}[workload]
	payload, _ := json.Marshal(map[string]string{
		"name":        fmt.Sprintf("%s-%04d", workload, index),
		"description": description,
		"command":     "sleep 2",
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/jobs", bytes.NewReader(payload))
	if err != nil {
		return result{err: err}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", runID, index))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	started := time.Now()
	response, err := client.Do(request)
	latency := time.Since(started)
	if err != nil {
		return result{latency: latency, err: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return result{latency: latency, err: fmt.Errorf("submission %d returned %s: %s", index, response.Status, body)}
	}
	var created job
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return result{latency: latency, err: err}
	}
	return result{job: created, latency: latency}
}

func waitForJobs(ctx context.Context, client *http.Client, baseURL, token string, jobs []job, concurrency int) map[string]int {
	work := make(chan job)
	states := make(chan string, len(jobs))
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for item := range work {
				states <- waitForJob(ctx, client, baseURL, token, item.ID)
			}
		}()
	}
	go func() {
		defer close(work)
		for _, item := range jobs {
			select {
			case <-ctx.Done():
				return
			case work <- item:
			}
		}
	}()
	go func() {
		workers.Wait()
		close(states)
	}()

	counts := map[string]int{}
	for state := range states {
		counts[state]++
	}
	return counts
}

func waitForJob(ctx context.Context, client *http.Client, baseURL, token, jobID string) string {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "UNFINISHED"
		case <-ticker.C:
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/jobs/"+jobID, nil)
			if err != nil {
				continue
			}
			if token != "" {
				request.Header.Set("Authorization", "Bearer "+token)
			}
			response, err := client.Do(request)
			if err != nil {
				continue
			}
			var current job
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&current)
			response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && (current.Status == "SUCCEEDED" || current.Status == "FAILED") {
				return current.Status
			}
		}
	}
}

func percentileMS(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * percentile)
	return float64(values[index].Microseconds()) / 1000
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
