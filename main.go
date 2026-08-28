package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Job struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	Description      string     `json:"description,omitempty"`
	Command          string     `json:"command"`
	Status           string     `json:"status"`
	WorkloadType     string     `json:"workload_type"`
	Priority         string     `json:"priority"`
	SchedulingReason string     `json:"scheduling_reason,omitempty"`
	WorkerID         string     `json:"worker_id,omitempty"`
	Output           string     `json:"output,omitempty"`
	Error            string     `json:"error,omitempty"`
	Attempts         int        `json:"attempts"`
	CreatedAt        time.Time  `json:"created_at"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	DurationMS       int64      `json:"duration_ms,omitempty"`
}

type CreateJobRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Command     string `json:"command"`
}

type ScheduleRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Command     string `json:"command"`
}

type SchedulingDecision struct {
	WorkloadType string `json:"workload_type"`
	Priority     string `json:"priority"`
	Reason       string `json:"reason"`
}

type GroqResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

var ctx = context.Background()

const (
	cpuQueue         = "taskgrid:jobs:cpu"
	ioQueue          = "taskgrid:jobs:io"
	generalQueue     = "taskgrid:jobs:general"
	processingPrefix = "taskgrid:processing:"
	heartbeatPrefix  = "taskgrid:heartbeat:"
	jobKeyPrefix     = "taskgrid:job:"
)

func getRedisAddress() string {
	addr := os.Getenv("REDIS_ADDR")

	if addr == "" {
		return "localhost:6379"
	}

	return addr
}

var rdb = redis.NewClient(&redis.Options{
	Addr: getRedisAddress(),
})

func jobQueueFor(workloadType string) string {
	switch strings.ToLower(strings.TrimSpace(workloadType)) {
	case "cpu":
		return cpuQueue

	case "io":
		return ioQueue

	default:
		return generalQueue
	}
}

func saveJob(job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return rdb.Set(
		ctx,
		jobKeyPrefix+job.ID,
		data,
		0,
	).Err()
}

func loadJob(jobID string) (Job, error) {
	data, err := rdb.Get(
		ctx,
		jobKeyPrefix+jobID,
	).Result()

	if err != nil {
		return Job{}, err
	}

	var job Job

	if err := json.Unmarshal(
		[]byte(data),
		&job,
	); err != nil {
		return Job{}, err
	}

	return job, nil
}

func scheduleWithAI(req ScheduleRequest) (SchedulingDecision, error) {
	apiKey := os.Getenv("GROQ_API_KEY")

	if apiKey == "" {
		return SchedulingDecision{},
			fmt.Errorf("GROQ_API_KEY is not configured")
	}

	model := os.Getenv("GROQ_MODEL")

	if model == "" {
		model = "openai/gpt-oss-20b"
	}

	systemPrompt := `
You are the scheduler for TaskGrid, a distributed Kubernetes job system.

Classify each incoming job.

workload_type must be exactly one of:
- cpu
- io
- general

priority must be exactly one of:
- high
- normal
- low

Definitions:

cpu:
Sustained computation such as compilation, rendering, encoding,
machine learning, mathematical processing, simulations, or other
CPU-heavy work.

io:
Network, file, database, storage, download, upload, backup,
waiting, or other input/output-heavy work.

general:
Lightweight, mixed, or miscellaneous workloads that do not clearly
belong to cpu or io.

Priority should represent how urgently the job should be executed.

Return only JSON using exactly this structure:

{
  "workload_type": "cpu|io|general",
  "priority": "high|normal|low",
  "reason": "one short sentence"
}
`

	userPrompt := fmt.Sprintf(
		"Name: %s\nDescription: %s\nCommand: %s",
		req.Name,
		req.Description,
		req.Command,
	)

	requestBody := map[string]interface{}{
		"model": model,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": systemPrompt,
			},
			{
				"role":    "user",
				"content": userPrompt,
			},
		},
		"temperature": 0,
		"response_format": map[string]string{
			"type": "json_object",
		},
	}

	data, err := json.Marshal(requestBody)
	if err != nil {
		return SchedulingDecision{}, err
	}

	requestCtx, cancel := context.WithTimeout(
		ctx,
		15*time.Second,
	)

	defer cancel()

	httpReq, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		"https://api.groq.com/openai/v1/chat/completions",
		bytes.NewReader(data),
	)

	if err != nil {
		return SchedulingDecision{}, err
	}

	httpReq.Header.Set(
		"Authorization",
		"Bearer "+apiKey,
	)

	httpReq.Header.Set(
		"Content-Type",
		"application/json",
	)

	client := &http.Client{}

	resp, err := client.Do(httpReq)
	if err != nil {
		return SchedulingDecision{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {

		body, _ := io.ReadAll(resp.Body)

		return SchedulingDecision{},
			fmt.Errorf(
				"Groq returned %s: %s",
				resp.Status,
				string(body),
			)
	}

	var groqResponse GroqResponse

	if err := json.NewDecoder(
		resp.Body,
	).Decode(&groqResponse); err != nil {
		return SchedulingDecision{}, err
	}

	if len(groqResponse.Choices) == 0 {
		return SchedulingDecision{},
			fmt.Errorf("AI returned no scheduling decision")
	}

	var decision SchedulingDecision

	if err := json.Unmarshal(
		[]byte(
			groqResponse.
				Choices[0].
				Message.
				Content,
		),
		&decision,
	); err != nil {
		return SchedulingDecision{}, err
	}

	decision.WorkloadType = strings.ToLower(
		strings.TrimSpace(
			decision.WorkloadType,
		),
	)

	decision.Priority = strings.ToLower(
		strings.TrimSpace(
			decision.Priority,
		),
	)

	switch decision.WorkloadType {
	case "cpu", "io", "general":

	default:
		return SchedulingDecision{},
			fmt.Errorf(
				"invalid workload type from AI: %s",
				decision.WorkloadType,
			)
	}

	switch decision.Priority {
	case "high", "normal", "low":

	default:
		return SchedulingDecision{},
			fmt.Errorf(
				"invalid priority from AI: %s",
				decision.Priority,
			)
	}

	if strings.TrimSpace(decision.Reason) == "" {
		decision.Reason = "AI classified the workload."
	}

	return decision, nil
}

func healthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if err := rdb.Ping(ctx).Err(); err != nil {
		http.Error(
			w,
			"TaskGrid API cannot reach Redis",
			http.StatusServiceUnavailable,
		)

		return
	}

	fmt.Fprintln(
		w,
		"TaskGrid API is healthy",
	)
}

func scheduleHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)

		return
	}

	var req ScheduleRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(
			w,
			"invalid JSON request",
			http.StatusBadRequest,
		)

		return
	}

	if strings.TrimSpace(req.Description) == "" &&
		strings.TrimSpace(req.Command) == "" {

		http.Error(
			w,
			"description or command is required",
			http.StatusBadRequest,
		)

		return
	}

	decision, err := scheduleWithAI(req)
	if err != nil {
		log.Println(
			"AI scheduling failed:",
			err,
		)

		http.Error(
			w,
			"AI scheduling failed",
			http.StatusInternalServerError,
		)

		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(
		decision,
	); err != nil {
		log.Println(
			"failed to encode scheduling decision:",
			err,
		)
	}
}

func jobsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	switch r.Method {
	case http.MethodPost:
		createJobHandler(w, r)

	case http.MethodGet:
		listJobsHandler(w, r)

	default:
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
	}
}

func createJobHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req CreateJobRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(
			w,
			"invalid JSON request",
			http.StatusBadRequest,
		)

		return
	}

	if strings.TrimSpace(req.Name) == "" {
		http.Error(
			w,
			"name is required",
			http.StatusBadRequest,
		)

		return
	}

	if strings.TrimSpace(req.Command) == "" {
		http.Error(
			w,
			"command is required",
			http.StatusBadRequest,
		)

		return
	}

	decision, err := scheduleWithAI(
		ScheduleRequest{
			Name:        req.Name,
			Description: req.Description,
			Command:     req.Command,
		},
	)

	if err != nil {
		log.Println(
			"AI scheduler failed, using general queue:",
			err,
		)

		decision = SchedulingDecision{
			WorkloadType: "general",
			Priority:     "normal",
			Reason: "AI scheduler unavailable, " +
				"defaulted to general workload.",
		}
	}

	jobNumber, err := rdb.Incr(
		ctx,
		"taskgrid:next_job_id",
	).Result()

	if err != nil {
		http.Error(
			w,
			"failed to generate job ID",
			http.StatusInternalServerError,
		)

		return
	}

	jobID := fmt.Sprintf(
		"job-%d",
		jobNumber,
	)

	job := Job{
		ID:               jobID,
		Name:             req.Name,
		Description:      req.Description,
		Command:          req.Command,
		Status:           "QUEUED",
		WorkloadType:     decision.WorkloadType,
		Priority:         decision.Priority,
		SchedulingReason: decision.Reason,
		Attempts:         0,
		CreatedAt:        time.Now().UTC(),
	}

	if err := saveJob(job); err != nil {
		http.Error(
			w,
			"failed to save job",
			http.StatusInternalServerError,
		)

		return
	}

	queue := jobQueueFor(
		job.WorkloadType,
	)

	if err := rdb.LPush(
		ctx,
		queue,
		job.ID,
	).Err(); err != nil {

		http.Error(
			w,
			"failed to enqueue job",
			http.StatusInternalServerError,
		)

		return
	}

	log.Println(
		"AI routed",
		job.ID,
		"to",
		queue,
		"with priority",
		job.Priority,
	)

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(
		http.StatusCreated,
	)

	if err := json.NewEncoder(w).Encode(
		job,
	); err != nil {
		log.Println(
			"failed to encode response:",
			err,
		)
	}
}

func listJobsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	var cursor uint64

	jobList := make(
		[]Job,
		0,
	)

	for {
		keys, nextCursor, err := rdb.Scan(
			ctx,
			cursor,
			jobKeyPrefix+"*",
			100,
		).Result()

		if err != nil {
			http.Error(
				w,
				"failed to list jobs",
				http.StatusInternalServerError,
			)

			return
		}

		for _, key := range keys {
			data, err := rdb.Get(
				ctx,
				key,
			).Result()

			if err != nil {
				log.Println(
					"failed to load",
					key,
					":",
					err,
				)

				continue
			}

			var job Job

			if err := json.Unmarshal(
				[]byte(data),
				&job,
			); err != nil {

				log.Println(
					"failed to decode",
					key,
					":",
					err,
				)

				continue
			}

			jobList = append(
				jobList,
				job,
			)
		}

		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}

	sort.Slice(
		jobList,
		func(i int, j int) bool {
			return jobList[i].
				CreatedAt.
				Before(
					jobList[j].
						CreatedAt,
				)
		},
	)

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(
		jobList,
	); err != nil {
		log.Println(
			"failed to encode job list:",
			err,
		)
	}
}

func jobByIDHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)

		return
	}

	jobID := strings.TrimPrefix(
		r.URL.Path,
		"/jobs/",
	)

	if jobID == "" {
		http.Error(
			w,
			"job ID is required",
			http.StatusBadRequest,
		)

		return
	}

	job, err := loadJob(jobID)

	if err != nil {
		if err == redis.Nil {
			http.Error(
				w,
				"job not found",
				http.StatusNotFound,
			)

			return
		}

		http.Error(
			w,
			"failed to load job",
			http.StatusInternalServerError,
		)

		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(
		job,
	); err != nil {
		log.Println(
			"failed to encode response:",
			err,
		)
	}
}

func startHeartbeat(workerID string) {
	heartbeatKey := heartbeatPrefix +
		workerID

	updateHeartbeat := func() {
		err := rdb.Set(
			ctx,
			heartbeatKey,
			"alive",
			10*time.Second,
		).Err()

		if err != nil {
			log.Println(
				workerID,
				"failed to update heartbeat:",
				err,
			)
		}
	}

	updateHeartbeat()

	ticker := time.NewTicker(
		3 * time.Second,
	)

	go func() {
		defer ticker.Stop()

		for range ticker.C {
			updateHeartbeat()
		}
	}()
}

func startRecoveryLoop() {
	ticker := time.NewTicker(
		5 * time.Second,
	)

	go func() {
		defer ticker.Stop()

		for range ticker.C {
			recoverDeadWorkers()
		}
	}()
}

func recoverDeadWorkers() {
	var cursor uint64

	for {
		keys, nextCursor, err := rdb.Scan(
			ctx,
			cursor,
			processingPrefix+"*",
			100,
		).Result()

		if err != nil {
			log.Println(
				"recovery scan failed:",
				err,
			)

			return
		}

		for _, processingKey := range keys {
			workerID := strings.TrimPrefix(
				processingKey,
				processingPrefix,
			)

			heartbeatKey := heartbeatPrefix +
				workerID

			exists, err := rdb.Exists(
				ctx,
				heartbeatKey,
			).Result()

			if err != nil {
				continue
			}

			if exists == 1 {
				continue
			}

			recoverProcessingQueue(
				workerID,
				processingKey,
			)
		}

		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}
}

func recoverProcessingQueue(
	workerID string,
	processingKey string,
) {
	jobIDs, err := rdb.LRange(
		ctx,
		processingKey,
		0,
		-1,
	).Result()

	if err != nil {
		log.Println(
			"failed reading abandoned jobs:",
			err,
		)

		return
	}

	requeueScript := redis.NewScript(`
		local removed = redis.call(
			"LREM",
			KEYS[1],
			1,
			ARGV[1]
		)

		if removed == 1 then
			redis.call(
				"LPUSH",
				KEYS[2],
				ARGV[1]
			)
		end

		return removed
	`)

	for _, jobID := range jobIDs {
		job, err := loadJob(jobID)

		if err != nil {
			log.Println(
				"failed loading abandoned",
				jobID,
				":",
				err,
			)

			continue
		}

		// If the job already completed but its
		// acknowledgement failed, do not run it again.
		if job.Status == "SUCCEEDED" ||
			job.Status == "FAILED" {

			err := rdb.LRem(
				ctx,
				processingKey,
				1,
				jobID,
			).Err()

			if err != nil {
				log.Println(
					"failed cleaning completed",
					jobID,
					":",
					err,
				)
			}

			continue
		}

		targetQueue := jobQueueFor(
			job.WorkloadType,
		)

		job.Status = "QUEUED"
		job.WorkerID = ""
		job.StartedAt = nil
		job.FinishedAt = nil
		job.DurationMS = 0
		job.Output = ""
		job.Error = "recovered after worker failure"

		if err := saveJob(job); err != nil {
			log.Println(
				"failed updating recovered",
				jobID,
				":",
				err,
			)

			continue
		}

		moved, err := requeueScript.Run(
			ctx,
			rdb,
			[]string{
				processingKey,
				targetQueue,
			},
			jobID,
		).Int()

		if err != nil {
			log.Println(
				"failed recovering",
				jobID,
				":",
				err,
			)

			continue
		}

		if moved == 1 {
			log.Println(
				"recovered",
				jobID,
				"from dead worker",
				workerID,
				"back to",
				targetQueue,
			)
		}
	}
}

func requeueClaimedJob(
	processingQueue string,
	targetQueue string,
	jobID string,
) {
	script := redis.NewScript(`
		local removed = redis.call(
			"LREM",
			KEYS[1],
			1,
			ARGV[1]
		)

		if removed == 1 then
			redis.call(
				"LPUSH",
				KEYS[2],
				ARGV[1]
			)
		end

		return removed
	`)

	_, err := script.Run(
		ctx,
		rdb,
		[]string{
			processingQueue,
			targetQueue,
		},
		jobID,
	).Result()

	if err != nil {
		log.Println(
			"failed to requeue",
			jobID,
			":",
			err,
		)
	}
}

func worker(
	workerID string,
	workerType string,
) {
	workerType = strings.ToLower(
		strings.TrimSpace(workerType),
	)

	switch workerType {
	case "cpu", "io", "general":

	default:
		workerType = "general"
	}

	startHeartbeat(workerID)
	startRecoveryLoop()

	queue := jobQueueFor(workerType)

	processingQueue := processingPrefix +
		workerID

	log.Println(
		workerID,
		"started as",
		workerType,
		"worker and is waiting on",
		queue,
	)

	for {
		jobID, err := rdb.BRPopLPush(
			ctx,
			queue,
			processingQueue,
			0,
		).Result()

		if err != nil {
			log.Println(
				workerID,
				"Redis error:",
				err,
			)

			continue
		}

		log.Println(
			workerID,
			"received",
			jobID,
		)

		job, err := loadJob(jobID)

		if err != nil {
			log.Println(
				workerID,
				"failed to load",
				jobID,
				":",
				err,
			)

			rdb.LRem(
				ctx,
				processingQueue,
				1,
				jobID,
			)

			continue
		}

		job.Status = "RUNNING"
		job.WorkerID = workerID
		job.Output = ""
		job.Error = ""
		job.FinishedAt = nil
		job.DurationMS = 0
		job.Attempts++

		startedAt := time.Now().UTC()

		job.StartedAt = &startedAt

		if err := saveJob(job); err != nil {
			log.Println(
				workerID,
				"failed to save",
				jobID,
				":",
				err,
			)

			requeueClaimedJob(
				processingQueue,
				queue,
				jobID,
			)

			continue
		}

		log.Println(
			workerID,
			"is running",
			jobID,
			"type",
			job.WorkloadType,
			"attempt",
			job.Attempts,
		)

		commandCtx, cancel := context.WithTimeout(
			ctx,
			30*time.Second,
		)

		cmd := exec.CommandContext(
			commandCtx,
			"bash",
			"-lc",
			job.Command,
		)

		output, commandErr := cmd.CombinedOutput()

		timedOut := commandCtx.Err() ==
			context.DeadlineExceeded

		cancel()

		job.Output = string(output)

		finishedAt := time.Now().UTC()

		job.FinishedAt = &finishedAt

		job.DurationMS = finishedAt.
			Sub(*job.StartedAt).
			Milliseconds()

		if timedOut {
			job.Status = "FAILED"
			job.Error = "command timed out after 30 seconds"

		} else if commandErr != nil {
			job.Status = "FAILED"
			job.Error = commandErr.Error()

		} else {
			job.Status = "SUCCEEDED"
			job.Error = ""
		}

		if err := saveJob(job); err != nil {
			log.Println(
				workerID,
				"failed to save completed job",
				jobID,
				":",
				err,
			)

			continue
		}

		if err := rdb.LRem(
			ctx,
			processingQueue,
			1,
			jobID,
		).Err(); err != nil {

			log.Println(
				workerID,
				"failed to acknowledge",
				jobID,
				":",
				err,
			)

			continue
		}

		log.Println(
			workerID,
			"finished",
			jobID,
			"with status",
			job.Status,
		)
	}
}

func startAPI() {
	http.HandleFunc(
		"/health",
		healthHandler,
	)

	http.HandleFunc(
		"/schedule",
		scheduleHandler,
	)

	http.HandleFunc(
		"/jobs",
		jobsHandler,
	)

	http.HandleFunc(
		"/jobs/",
		jobByIDHandler,
	)

	log.Println(
		"TaskGrid API listening on port 8080",
	)

	err := http.ListenAndServe(
		":8080",
		nil,
	)

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal(
			"could not connect to Redis:",
			err,
		)
	}

	log.Println(
		"connected to Redis",
	)

	mode := strings.ToLower(
		strings.TrimSpace(
			os.Getenv("TASKGRID_MODE"),
		),
	)

	if mode == "" {
		mode = "all"
	}

	switch mode {
	case "api":
		log.Println(
			"starting in API mode",
		)

		startAPI()

	case "worker":
		workerID := os.Getenv(
			"WORKER_ID",
		)

		if workerID == "" {
			hostname, err := os.Hostname()

			if err != nil {
				workerID = "worker"
			} else {
				workerID = hostname
			}
		}

		workerType := os.Getenv(
			"WORKER_TYPE",
		)

		if workerType == "" {
			workerType = "general"
		}

		log.Println(
			"starting in worker mode as",
			workerID,
			"type",
			workerType,
		)

		worker(
			workerID,
			workerType,
		)

	case "all":
		log.Println(
			"starting in local development mode",
		)

		go worker(
			"worker-cpu",
			"cpu",
		)

		go worker(
			"worker-io",
			"io",
		)

		go worker(
			"worker-general",
			"general",
		)

		startAPI()

	default:
		log.Fatal(
			"invalid TASKGRID_MODE:",
			mode,
		)
	}
}
