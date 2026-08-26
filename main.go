package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Job struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Command   string    `json:"command"`
	Status    string    `json:"status"`
	WorkerID  string    `json:"worker_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateJobRequest struct {
	Name    string `json:"name"`
	Command string `json:"command"`
}

var (
	jobs      = make(map[string]Job)
	jobsMu    sync.Mutex
	nextJobID = 1
	jobQueue  = make(chan string, 100)
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "TaskGrid API is healthy")
}

func jobsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateJobRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid JSON request", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Name) == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Command) == "" {
		http.Error(w, "command is required", http.StatusBadRequest)
		return
	}

	jobsMu.Lock()

	jobID := fmt.Sprintf("job-%d", nextJobID)
	nextJobID++

	job := Job{
		ID:        jobID,
		Name:      req.Name,
		Command:   req.Command,
		Status:    "QUEUED",
		CreatedAt: time.Now().UTC(),
	}

	jobs[job.ID] = job

	jobsMu.Unlock()

	jobQueue <- job.ID

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Println("failed to encode response:", err)
	}
}

func jobByIDHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")

	if jobID == "" {
		http.Error(w, "job ID is required", http.StatusBadRequest)
		return
	}

	jobsMu.Lock()
	job, exists := jobs[jobID]
	jobsMu.Unlock()

	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Println("failed to encode response:", err)
	}
}
func worker(workerID string) {
	log.Println(workerID, "started and waiting for jobs")

	for jobID := range jobQueue {
		log.Println(workerID, "received", jobID)

		jobsMu.Lock()

		job := jobs[jobID]
		job.Status = "RUNNING"
		job.WorkerID = workerID
		jobs[jobID] = job

		jobsMu.Unlock()

		log.Println(workerID, "is running", jobID)

		time.Sleep(5 * time.Second)

		jobsMu.Lock()

		job = jobs[jobID]
		job.Status = "SUCCEEDED"
		jobs[jobID] = job

		jobsMu.Unlock()

		log.Println(workerID, "completed", jobID)
	}
}
func main() {
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/jobs", jobsHandler)
	http.HandleFunc("/jobs/", jobByIDHandler)

	go worker("worker-1")
	go worker("worker-2")
	go worker("worker-3")

	log.Println("TaskGrid API listening on port 8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
