package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	LastError        string     `json:"last_error,omitempty"`
	Attempts         int        `json:"attempts"`
	MaxAttempts      int        `json:"max_attempts"`
	OutputTruncated  bool       `json:"output_truncated,omitempty"`
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

var ctx, cancel = context.WithCancel(context.Background())

var (
	requestsTotal      atomic.Uint64
	responses2xx       atomic.Uint64
	responses4xx       atomic.Uint64
	responses5xx       atomic.Uint64
	authFailures       atomic.Uint64
	rateLimited        atomic.Uint64
	aiRequests         atomic.Uint64
	aiFallbacks        atomic.Uint64
	authDisabledNotice sync.Once
	recoveryLoopOnce   sync.Once
	latencyBuckets     = [...]float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	latencyCounts      [9]atomic.Uint64
	latencyMicrosTotal atomic.Uint64
)

const (
	cpuQueue          = "taskgrid:jobs:cpu"
	ioQueue           = "taskgrid:jobs:io"
	generalQueue      = "taskgrid:jobs:general"
	processingPrefix  = "taskgrid:processing:"
	heartbeatPrefix   = "taskgrid:heartbeat:"
	jobKeyPrefix      = "taskgrid:job:"
	idempotencyPrefix = "taskgrid:idempotency:"
	jobIndexKey       = "taskgrid:jobs:index"
	deadLetterQueue   = "taskgrid:jobs:dead"
)

type contextKey string

const roleContextKey contextKey = "taskgrid-role"

type statusRecorder struct {
	http.ResponseWriter
	status int
}

type limitedBuffer struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	originalLength := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining <= 0 {
		b.truncated = true
		return originalLength, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, _ = b.buffer.Write(p)
	return originalLength, nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

func envInt(name string, fallback, minimum, maximum int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		log.Printf("WARNING: %s=%q is invalid; using %d", name, value, fallback)
		return fallback
	}
	return parsed
}

func maxAttempts() int {
	return envInt("TASKGRID_MAX_ATTEMPTS", 3, 1, 20)
}

func jobTimeout() time.Duration {
	return time.Duration(envInt("TASKGRID_JOB_TIMEOUT_SECONDS", 30, 1, 3600)) * time.Second
}

func maxOutputBytes() int {
	return envInt("TASKGRID_MAX_OUTPUT_BYTES", 65536, 1024, 10485760)
}

func jobRetention() time.Duration {
	return time.Duration(envInt("TASKGRID_JOB_RETENTION_HOURS", 168, 1, 8760)) * time.Hour
}

func rateLimitPerMinute() int {
	return envInt("TASKGRID_RATE_LIMIT_PER_MINUTE", 0, 0, 100000)
}

func getRedisAddress() string {
	addr := os.Getenv("REDIS_ADDR")

	if addr == "" {
		return "localhost:6379"
	}

	return addr
}

func apiToken() string {
	return os.Getenv("TASKGRID_API_TOKEN")
}

func adminToken() string {
	return strings.TrimSpace(os.Getenv("TASKGRID_ADMIN_TOKEN"))
}

func submitToken() string {
	return strings.TrimSpace(os.Getenv("TASKGRID_SUBMIT_TOKEN"))
}

func readToken() string {
	return strings.TrimSpace(os.Getenv("TASKGRID_READ_TOKEN"))
}

func authRequired() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("TASKGRID_REQUIRE_AUTH")), "true")
}

func validateAuthConfig() error {
	tokens := map[string]string{
		"legacy-admin": strings.TrimSpace(apiToken()),
		"admin":        adminToken(),
		"submit":       submitToken(),
		"read":         readToken(),
	}
	seen := make(map[string]string)
	for role, token := range tokens {
		if token == "" {
			continue
		}
		if otherRole, exists := seen[token]; exists {
			return fmt.Errorf("%s and %s API tokens must be different", otherRole, role)
		}
		seen[token] = role
	}
	if authRequired() && len(seen) == 0 {
		return errors.New("TASKGRID_REQUIRE_AUTH=true but no API token is configured")
	}
	return nil
}

func shellCommandsAllowed() bool {
	return strings.EqualFold(os.Getenv("TASKGRID_ALLOW_SHELL_COMMANDS"), "true")
}

func parseCommand(command string) (string, []string, error) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "", nil, errors.New("command is required")
	}

	executable := parts[0]
	allowedValue := strings.TrimSpace(os.Getenv("TASKGRID_ALLOWED_COMMANDS"))
	if allowedValue != "" {
		allowed := false
		for _, candidate := range strings.Split(allowedValue, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), executable) {
				allowed = true
				break
			}
		}
		if !allowed {
			return "", nil, fmt.Errorf("executable %q is not in TASKGRID_ALLOWED_COMMANDS", executable)
		}
	}

	return executable, parts[1:], nil
}

func schedulerMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("TASKGRID_SCHEDULER_MODE")))
	if mode == "" {
		return "hybrid"
	}
	return mode
}

func scheduleWithRules(req ScheduleRequest) SchedulingDecision {
	text := strings.ToLower(req.Name + " " + req.Description + " " + req.Command)
	description := strings.ToLower(req.Description)
	for _, workloadType := range []string{"cpu", "io", "general"} {
		if strings.Contains(description, "[workload:"+workloadType+"]") {
			return SchedulingDecision{
				WorkloadType: workloadType,
				Priority:     "normal",
				Reason:       "Deterministic rules honored an explicit workload hint.",
			}
		}
	}

	cpuTerms := []string{"compile", "encode", "render", "calculate", "compute", "simulation", "machine learning", "sha256", "benchmark"}
	for _, term := range cpuTerms {
		if strings.Contains(text, term) {
			return SchedulingDecision{WorkloadType: "cpu", Priority: "normal", Reason: "Deterministic rules identified a compute-heavy workload."}
		}
	}

	ioTerms := []string{"download", "upload", "network", "database", "backup", "file", "storage", "sleep", "wait"}
	for _, term := range ioTerms {
		if strings.Contains(text, term) {
			return SchedulingDecision{WorkloadType: "io", Priority: "normal", Reason: "Deterministic rules identified an I/O or waiting workload."}
		}
	}

	return SchedulingDecision{WorkloadType: "general", Priority: "normal", Reason: "Deterministic rules selected the general workload pool."}
}

func scheduleJob(req ScheduleRequest) (SchedulingDecision, error) {
	switch schedulerMode() {
	case "rules":
		return scheduleWithRules(req), nil
	case "ai":
		return scheduleWithAI(req)
	case "hybrid":
		decision, err := scheduleWithAI(req)
		if err == nil {
			return decision, nil
		}
		aiFallbacks.Add(1)
		log.Printf("AI scheduler unavailable; using deterministic rules: %v", err)
		return scheduleWithRules(req), nil
	default:
		return SchedulingDecision{}, fmt.Errorf("invalid TASKGRID_SCHEDULER_MODE %q", schedulerMode())
	}
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
		jobRetention(),
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

func idempotencyRedisKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%s%x", idempotencyPrefix, digest)
}

func loadIdempotentJob(value string) (Job, bool, error) {
	jobID, err := rdb.Get(ctx, idempotencyRedisKey(value)).Result()
	if err == redis.Nil {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}

	job, err := loadJob(jobID)
	if err == redis.Nil {
		_ = rdb.Del(ctx, idempotencyRedisKey(value)).Err()
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func enqueueJob(job Job, queue, idempotencyKey string) (bool, string, error) {
	data, err := json.Marshal(job)
	if err != nil {
		return false, "", err
	}

	hasIdempotency := "0"
	idempotencyRedis := idempotencyPrefix + "none"
	if idempotencyKey != "" {
		hasIdempotency = "1"
		idempotencyRedis = idempotencyRedisKey(idempotencyKey)
	}

	script := redis.NewScript(`
		if ARGV[5] == "1" then
			local existing = redis.call("GET", KEYS[3])
			if existing then
				return {0, existing}
			end
		end
		redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
		redis.call("LPUSH", KEYS[2], ARGV[3])
		redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", ARGV[6])
		redis.call("ZADD", KEYS[4], ARGV[4], ARGV[3])
		redis.call("INCR", KEYS[5])
		if ARGV[5] == "1" then
			redis.call("SET", KEYS[3], ARGV[3], "EX", ARGV[2])
		end
		return {1, ARGV[3]}
	`)

	result, err := script.Run(ctx, rdb, []string{
		jobKeyPrefix + job.ID,
		queue,
		idempotencyRedis,
		jobIndexKey,
		"taskgrid:metrics:jobs_created_total",
	}, string(data), int64(jobRetention().Seconds()), job.ID, job.CreatedAt.UnixMilli(), hasIdempotency, time.Now().Add(-jobRetention()).UnixMilli()).Slice()
	if err != nil {
		return false, "", err
	}
	if len(result) != 2 {
		return false, "", fmt.Errorf("unexpected enqueue result")
	}

	created, ok := result[0].(int64)
	if !ok {
		return false, "", fmt.Errorf("unexpected enqueue status")
	}
	jobID, ok := result[1].(string)
	if !ok {
		return false, "", fmt.Errorf("unexpected enqueue job ID")
	}
	return created == 1, jobID, nil
}

func persistAndMove(job Job, processingQueue, targetQueue, metricKey string) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	script := redis.NewScript(`
		redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
		local removed = redis.call("LREM", KEYS[2], 1, ARGV[3])
		if removed == 1 and KEYS[3] ~= "" then
			redis.call("LPUSH", KEYS[3], ARGV[3])
		end
		if KEYS[4] ~= "" then
			redis.call("INCR", KEYS[4])
		end
		return removed
	`)

	moved, err := script.Run(ctx, rdb, []string{
		jobKeyPrefix + job.ID,
		processingQueue,
		targetQueue,
		metricKey,
	}, string(data), int64(jobRetention().Seconds()), job.ID).Int()
	if err != nil {
		return err
	}
	if moved != 1 {
		return fmt.Errorf("job %s was not present in processing queue %s", job.ID, processingQueue)
	}
	return nil
}

func scheduleWithAI(req ScheduleRequest) (SchedulingDecision, error) {
	aiRequests.Add(1)
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

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

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
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "TaskGrid API is alive")
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	if err := rdb.Ping(r.Context()).Err(); err != nil {
		http.Error(w, "TaskGrid API is not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "TaskGrid API is ready")
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	redisUp := 1
	if err := rdb.Ping(r.Context()).Err(); err != nil {
		redisUp = 0
	}
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_redis_up Whether the API can reach Redis.\n# TYPE taskgrid_redis_up gauge\ntaskgrid_redis_up %d\n", redisUp)
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_http_requests_total Total HTTP requests handled.\n# TYPE taskgrid_http_requests_total counter\ntaskgrid_http_requests_total %d\n", requestsTotal.Load())
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_http_responses_total HTTP responses by status class.\n# TYPE taskgrid_http_responses_total counter\ntaskgrid_http_responses_total{class=\"2xx\"} %d\ntaskgrid_http_responses_total{class=\"4xx\"} %d\ntaskgrid_http_responses_total{class=\"5xx\"} %d\n", responses2xx.Load(), responses4xx.Load(), responses5xx.Load())
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_auth_failures_total Rejected authentication or authorization attempts.\n# TYPE taskgrid_auth_failures_total counter\ntaskgrid_auth_failures_total %d\n", authFailures.Load())
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_rate_limited_total Requests rejected by the per-credential rate limit.\n# TYPE taskgrid_rate_limited_total counter\ntaskgrid_rate_limited_total %d\n", rateLimited.Load())
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_ai_schedule_requests_total Scheduling requests sent to the AI provider.\n# TYPE taskgrid_ai_schedule_requests_total counter\ntaskgrid_ai_schedule_requests_total %d\n", aiRequests.Load())
	_, _ = fmt.Fprintf(w, "# HELP taskgrid_ai_fallbacks_total AI scheduling failures handled by deterministic rules.\n# TYPE taskgrid_ai_fallbacks_total counter\ntaskgrid_ai_fallbacks_total %d\n", aiFallbacks.Load())

	_, _ = fmt.Fprintln(w, "# HELP taskgrid_http_request_duration_seconds Request latency histogram.")
	_, _ = fmt.Fprintln(w, "# TYPE taskgrid_http_request_duration_seconds histogram")
	for i, upperBound := range latencyBuckets {
		_, _ = fmt.Fprintf(w, "taskgrid_http_request_duration_seconds_bucket{le=\"%g\"} %d\n", upperBound, latencyCounts[i].Load())
	}
	_, _ = fmt.Fprintf(w, "taskgrid_http_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", latencyCounts[len(latencyCounts)-1].Load())
	_, _ = fmt.Fprintf(w, "taskgrid_http_request_duration_seconds_sum %.6f\n", float64(latencyMicrosTotal.Load())/1_000_000)
	_, _ = fmt.Fprintf(w, "taskgrid_http_request_duration_seconds_count %d\n", latencyCounts[len(latencyCounts)-1].Load())

	redisMetrics := []struct {
		name string
		help string
		key  string
	}{
		{"taskgrid_jobs_created_total", "Total jobs accepted.", "taskgrid:metrics:jobs_created_total"},
		{"taskgrid_jobs_succeeded_total", "Total jobs completed successfully.", "taskgrid:metrics:jobs_succeeded_total"},
		{"taskgrid_jobs_failed_total", "Total jobs sent to the dead-letter queue.", "taskgrid:metrics:jobs_failed_total"},
		{"taskgrid_job_retries_total", "Total job attempts requeued for retry.", "taskgrid:metrics:job_retries_total"},
		{"taskgrid_job_recoveries_total", "Total jobs recovered after a worker failure.", "taskgrid:metrics:job_recoveries_total"},
	}
	for _, metric := range redisMetrics {
		value, err := rdb.Get(r.Context(), metric.key).Uint64()
		if err != nil && err != redis.Nil {
			continue
		}
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", metric.name, metric.help, metric.name, metric.name, value)
	}

	_, _ = fmt.Fprintln(w, "# HELP taskgrid_queue_depth Current Redis queue depth.")
	_, _ = fmt.Fprintln(w, "# TYPE taskgrid_queue_depth gauge")
	queues := []struct {
		label string
		key   string
	}{
		{"cpu", cpuQueue},
		{"io", ioQueue},
		{"general", generalQueue},
		{"dead_letter", deadLetterQueue},
	}
	for _, queue := range queues {
		depth, err := rdb.LLen(r.Context(), queue.key).Result()
		if err == nil {
			_, _ = fmt.Fprintf(w, "taskgrid_queue_depth{queue=\"%s\"} %d\n", queue.label, depth)
		}
	}
}

func withAPIProtection(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		requestsTotal.Add(1)
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" || len(requestID) > 128 {
			requestID = fmt.Sprintf("req-%d", time.Now().UTC().UnixNano())
		}
		w.Header().Set("X-Request-ID", requestID)
		recorder := &statusRecorder{ResponseWriter: w}
		role := "anonymous"

		// Health, readiness, and metrics must remain reachable by orchestrators.
		if r.URL.Path == "/health" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(recorder, r)
		} else {
			configured := apiToken() != "" || adminToken() != "" || submitToken() != "" || readToken() != ""
			if !configured {
				authDisabledNotice.Do(func() {
					log.Println("WARNING: no TaskGrid API tokens are configured; authentication is disabled")
				})
				role = "admin"
			} else {
				provided := r.Header.Get("Authorization")
				const prefix = "Bearer "
				if !strings.HasPrefix(provided, prefix) {
					authFailures.Add(1)
					w.Header().Set("WWW-Authenticate", "Bearer")
					http.Error(recorder, "unauthorized", http.StatusUnauthorized)
					finishRequest(recorder, r, requestID, role, started)
					return
				}

				token := strings.TrimPrefix(provided, prefix)
				switch {
				case tokenMatches(token, adminToken()):
					role = "admin"
				case tokenMatches(token, strings.TrimSpace(apiToken())):
					role = "admin"
				case tokenMatches(token, submitToken()):
					role = "submit"
				case tokenMatches(token, readToken()):
					role = "read"
				default:
					authFailures.Add(1)
					w.Header().Set("WWW-Authenticate", "Bearer")
					http.Error(recorder, "unauthorized", http.StatusUnauthorized)
					finishRequest(recorder, r, requestID, role, started)
					return
				}

				allowed, err := withinRateLimit(r.Context(), token)
				if err != nil {
					http.Error(recorder, "rate limiter unavailable", http.StatusServiceUnavailable)
					finishRequest(recorder, r, requestID, role, started)
					return
				}
				if !allowed {
					rateLimited.Add(1)
					w.Header().Set("Retry-After", "60")
					http.Error(recorder, "rate limit exceeded", http.StatusTooManyRequests)
					finishRequest(recorder, r, requestID, role, started)
					return
				}
			}

			if !roleCanAccess(role, r) {
				authFailures.Add(1)
				http.Error(recorder, "forbidden", http.StatusForbidden)
				finishRequest(recorder, r, requestID, role, started)
				return
			}

			r = r.WithContext(context.WithValue(r.Context(), roleContextKey, role))
			next.ServeHTTP(recorder, r)
		}

		finishRequest(recorder, r, requestID, role, started)
	})
}

func withinRateLimit(requestContext context.Context, token string) (bool, error) {
	limit := rateLimitPerMinute()
	if limit == 0 {
		return true, nil
	}
	digest := sha256.Sum256([]byte(token))
	window := time.Now().UTC().Unix() / 60
	key := fmt.Sprintf("taskgrid:ratelimit:%x:%d", digest, window)
	script := redis.NewScript(`
		local count = redis.call("INCR", KEYS[1])
		if count == 1 then
			redis.call("EXPIRE", KEYS[1], 120)
		end
		return count
	`)
	count, err := script.Run(requestContext, rdb, []string{key}).Int()
	if err != nil {
		return false, err
	}
	return count <= limit, nil
}

func tokenMatches(provided, expected string) bool {
	return expected != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}

func roleCanAccess(role string, r *http.Request) bool {
	if role == "admin" {
		return true
	}
	if role == "submit" {
		return r.Method == http.MethodPost && (r.URL.Path == "/jobs" || r.URL.Path == "/schedule")
	}
	if role == "read" {
		return r.Method == http.MethodGet && (r.URL.Path == "/jobs" || strings.HasPrefix(r.URL.Path, "/jobs/"))
	}
	return false
}

func finishRequest(recorder *statusRecorder, r *http.Request, requestID, role string, started time.Time) {
	status := recorder.status
	if status == 0 {
		status = http.StatusOK
	}
	switch {
	case status >= 200 && status < 300:
		responses2xx.Add(1)
	case status >= 400 && status < 500:
		responses4xx.Add(1)
	case status >= 500:
		responses5xx.Add(1)
	}

	duration := time.Since(started)
	latencyMicrosTotal.Add(uint64(duration.Microseconds()))
	seconds := duration.Seconds()
	for i, upperBound := range latencyBuckets {
		if seconds <= upperBound {
			latencyCounts[i].Add(1)
		}
	}
	latencyCounts[len(latencyCounts)-1].Add(1)

	entry, _ := json.Marshal(map[string]interface{}{
		"event":       "http_request",
		"request_id":  requestID,
		"method":      r.Method,
		"path":        r.URL.Path,
		"status":      status,
		"role":        role,
		"duration_ms": duration.Milliseconds(),
	})
	log.Print(string(entry))
}

func decodeJSONRequest(w http.ResponseWriter, r *http.Request, destination interface{}) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		http.Error(w, "invalid JSON request", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request body must contain one JSON object", http.StatusBadRequest)
		return false
	}
	return true
}

func validateScheduleRequest(req ScheduleRequest) error {
	if len(req.Name) > 200 {
		return errors.New("name must be 200 characters or fewer")
	}
	if len(req.Description) > 4000 {
		return errors.New("description must be 4000 characters or fewer")
	}
	if len(req.Command) > 2000 {
		return errors.New("command must be 2000 characters or fewer")
	}
	return nil
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

	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if err := validateScheduleRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

	decision, err := scheduleJob(req)
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

	if !decodeJSONRequest(w, r, &req) {
		return
	}
	if err := validateScheduleRequest(ScheduleRequest(req)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 128 {
		http.Error(w, "Idempotency-Key must be 128 characters or fewer", http.StatusBadRequest)
		return
	}
	if idempotencyKey != "" {
		existing, found, err := loadIdempotentJob(idempotencyKey)
		if err != nil {
			http.Error(w, "failed to resolve idempotency key", http.StatusInternalServerError)
			return
		}
		if found {
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, http.StatusOK, existing)
			return
		}
	}

	decision, err := scheduleJob(
		ScheduleRequest{
			Name:        req.Name,
			Description: req.Description,
			Command:     req.Command,
		},
	)

	if err != nil {
		log.Println("job scheduling failed:", err)
		http.Error(w, "job scheduling failed", http.StatusServiceUnavailable)
		return
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
		MaxAttempts:      maxAttempts(),
		CreatedAt:        time.Now().UTC(),
	}

	queue := jobQueueFor(
		job.WorkloadType,
	)

	created, resolvedJobID, err := enqueueJob(job, queue, idempotencyKey)
	if err != nil {
		http.Error(w, "failed to atomically enqueue job", http.StatusInternalServerError)
		return
	}
	if !created {
		existing, err := loadJob(resolvedJobID)
		if err != nil {
			http.Error(w, "failed to load idempotent job", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, existing)
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

	writeJSON(w, http.StatusCreated, job)
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Println("failed to encode response:", err)
	}
}

func listJobsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	limit := 50
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 200 {
			http.Error(w, "limit must be between 1 and 200", http.StatusBadRequest)
			return
		}
		limit = parsed
	}

	offset := 0
	if value := r.URL.Query().Get("offset"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 || parsed > 1_000_000 {
			http.Error(w, "offset must be between 0 and 1000000", http.StatusBadRequest)
			return
		}
		offset = parsed
	}

	jobIDs, err := rdb.ZRevRange(r.Context(), jobIndexKey, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}

	jobList := make([]Job, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job, err := loadJob(jobID)
		if err == redis.Nil {
			_ = rdb.ZRem(r.Context(), jobIndexKey, jobID).Err()
			continue
		}
		if job.MaxAttempts <= 0 {
			job.MaxAttempts = maxAttempts()
		}
		if err != nil {
			log.Printf("failed to load indexed job %s: %v", jobID, err)
			continue
		}
		jobList = append(jobList, job)
	}

	// Upgrade compatibility: older TaskGrid versions did not maintain the index.
	if len(jobIDs) == 0 && offset == 0 {
		legacyJobs, err := scanLegacyJobs(r.Context())
		if err != nil {
			http.Error(w, "failed to list jobs", http.StatusInternalServerError)
			return
		}
		if len(legacyJobs) > limit {
			legacyJobs = legacyJobs[:limit]
		}
		jobList = legacyJobs
	}

	w.Header().Set("X-Page-Limit", strconv.Itoa(limit))
	w.Header().Set("X-Page-Offset", strconv.Itoa(offset))
	writeJSON(w, http.StatusOK, jobList)
}

func scanLegacyJobs(requestContext context.Context) ([]Job, error) {
	var cursor uint64
	jobs := make([]Job, 0)
	for {
		keys, nextCursor, err := rdb.Scan(requestContext, cursor, jobKeyPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			data, err := rdb.Get(requestContext, key).Bytes()
			if err != nil {
				continue
			}
			var job Job
			if err := json.Unmarshal(data, &job); err != nil {
				continue
			}
			jobs = append(jobs, job)
			_ = rdb.ZAdd(requestContext, jobIndexKey, redis.Z{Score: float64(job.CreatedAt.UnixMilli()), Member: job.ID}).Err()
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	return jobs, nil
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
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updateHeartbeat()
			}
		}
	}()
}

func startRecoveryLoop() {
	recoveryLoopOnce.Do(func() {
		ticker := time.NewTicker(5 * time.Second)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					recoverDeadWorkers()
				}
			}
		}()
	})
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
		if job.MaxAttempts <= 0 {
			job.MaxAttempts = maxAttempts()
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

		if job.Attempts >= job.MaxAttempts {
			if err := deadLetterJob(&job, processingKey, "maximum attempts reached after worker failure"); err != nil {
				log.Printf("failed dead-lettering abandoned %s: %v", jobID, err)
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
		job.Error = ""
		job.LastError = "recovered after worker failure"

		if err := persistAndMove(job, processingKey, targetQueue, "taskgrid:metrics:job_recoveries_total"); err != nil {
			log.Printf("failed recovering %s: %v", jobID, err)
			continue
		}

		log.Printf("recovered %s from dead worker %s back to %s", jobID, workerID, targetQueue)
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
	workerType = strings.ToLower(strings.TrimSpace(workerType))
	switch workerType {
	case "cpu", "io", "general":
	default:
		workerType = "general"
	}

	startHeartbeat(workerID)
	startRecoveryLoop()

	queue := jobQueueFor(workerType)
	processingQueue := processingPrefix + workerID
	log.Printf("%s started as %s worker and is waiting on %s", workerID, workerType, queue)

	for {
		jobID, err := rdb.BRPopLPush(ctx, queue, processingQueue, 0).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("%s Redis error: %v", workerID, err)
			continue
		}

		log.Printf("%s received %s", workerID, jobID)
		job, err := loadJob(jobID)
		if err != nil {
			log.Printf("%s failed to load %s: %v", workerID, jobID, err)
			_ = rdb.LRem(ctx, processingQueue, 1, jobID).Err()
			continue
		}
		if job.MaxAttempts <= 0 {
			job.MaxAttempts = maxAttempts()
		}

		job.Status = "RUNNING"
		job.WorkerID = workerID
		job.Output = ""
		job.OutputTruncated = false
		job.Error = ""
		job.FinishedAt = nil
		job.DurationMS = 0
		job.Attempts++
		startedAt := time.Now().UTC()
		job.StartedAt = &startedAt

		if err := saveJob(job); err != nil {
			log.Printf("%s failed to mark %s running: %v", workerID, jobID, err)
			requeueClaimedJob(processingQueue, queue, jobID)
			continue
		}

		log.Printf("%s is running %s type=%s attempt=%d/%d", workerID, jobID, job.WorkloadType, job.Attempts, job.MaxAttempts)
		commandCtx, commandCancel := context.WithTimeout(ctx, jobTimeout())

		var cmd *exec.Cmd
		if shellCommandsAllowed() {
			cmd = exec.CommandContext(commandCtx, "bash", "-lc", job.Command)
		} else {
			executable, args, parseErr := parseCommand(job.Command)
			if parseErr != nil {
				commandCancel()
				failure := "invalid command: " + parseErr.Error()
				if err := deadLetterJob(&job, processingQueue, failure); err != nil {
					log.Printf("%s failed to dead-letter %s: %v", workerID, jobID, err)
				}
				continue
			}
			cmd = exec.CommandContext(commandCtx, executable, args...)
		}

		output := &limitedBuffer{limit: maxOutputBytes()}
		cmd.Stdout = output
		cmd.Stderr = output
		commandErr := cmd.Run()
		timedOut := errors.Is(commandCtx.Err(), context.DeadlineExceeded)
		shutdown := ctx.Err() != nil
		commandCancel()
		if shutdown {
			log.Printf("%s stopping with %s still claimed for recovery", workerID, jobID)
			return
		}

		job.Output = output.String()
		job.OutputTruncated = output.truncated
		finishedAt := time.Now().UTC()
		job.FinishedAt = &finishedAt
		job.DurationMS = finishedAt.Sub(*job.StartedAt).Milliseconds()

		if timedOut {
			failure := fmt.Sprintf("command timed out after %s", jobTimeout())
			handleRetryOrDeadLetter(&job, processingQueue, queue, failure)
			continue
		}
		if commandErr != nil {
			handleRetryOrDeadLetter(&job, processingQueue, queue, commandErr.Error())
			continue
		}

		job.Status = "SUCCEEDED"
		job.Error = ""
		job.LastError = ""
		if err := persistAndMove(job, processingQueue, "", "taskgrid:metrics:jobs_succeeded_total"); err != nil {
			log.Printf("%s failed to finalize %s: %v", workerID, jobID, err)
			continue
		}
		log.Printf("%s finished %s with status %s", workerID, jobID, job.Status)
	}
}

func handleRetryOrDeadLetter(job *Job, processingQueue, queue, failure string) {
	job.LastError = failure
	if job.Attempts < job.MaxAttempts {
		job.Status = "QUEUED"
		job.Error = ""
		job.WorkerID = ""
		job.StartedAt = nil
		job.FinishedAt = nil
		job.DurationMS = 0
		if err := persistAndMove(*job, processingQueue, queue, "taskgrid:metrics:job_retries_total"); err != nil {
			log.Printf("failed to requeue %s after attempt %d: %v", job.ID, job.Attempts, err)
			return
		}
		log.Printf("requeued %s after attempt %d/%d", job.ID, job.Attempts, job.MaxAttempts)
		return
	}

	if err := deadLetterJob(job, processingQueue, failure); err != nil {
		log.Printf("failed to dead-letter %s: %v", job.ID, err)
	}
}

func deadLetterJob(job *Job, processingQueue, failure string) error {
	finishedAt := time.Now().UTC()
	job.Status = "FAILED"
	job.Error = failure
	job.LastError = failure
	job.FinishedAt = &finishedAt
	if job.StartedAt != nil {
		job.DurationMS = finishedAt.Sub(*job.StartedAt).Milliseconds()
	}
	return persistAndMove(*job, processingQueue, deadLetterQueue, "taskgrid:metrics:jobs_failed_total")
}

func startAPI() {
	if err := validateAuthConfig(); err != nil {
		log.Fatal("invalid API authentication configuration: ", err)
	}
	if mode := schedulerMode(); mode != "ai" && mode != "hybrid" && mode != "rules" {
		log.Fatalf("invalid TASKGRID_SCHEDULER_MODE %q", mode)
	}
	// Keep recovery active even when KEDA has scaled every worker pool to zero.
	startRecoveryLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/readyz", readyHandler)
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/schedule", scheduleHandler)
	mux.HandleFunc("/jobs", jobsHandler)
	mux.HandleFunc("/jobs/", jobByIDHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           withAPIProtection(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		log.Println("shutting down TaskGrid API")
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Println("graceful shutdown failed:", err)
		}
	}()

	log.Println(
		"TaskGrid API listening on port 8080",
	)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func installCancellationSignal() {
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		cancel()
	}()
}

func main() {
	installCancellationSignal()
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
