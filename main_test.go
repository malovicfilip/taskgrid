package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestParseCommand(t *testing.T) {
	t.Setenv("TASKGRID_ALLOWED_COMMANDS", "echo,sleep")

	executable, args, err := parseCommand("echo taskgrid-ok")
	if err != nil {
		t.Fatalf("parseCommand returned an error: %v", err)
	}
	if executable != "echo" || len(args) != 1 || args[0] != "taskgrid-ok" {
		t.Fatalf("unexpected command: %q %#v", executable, args)
	}
}

func TestParseCommandRejectsEmptyAllowlist(t *testing.T) {
	t.Setenv("TASKGRID_ALLOWED_COMMANDS", "")

	if _, _, err := parseCommand("echo taskgrid-ok"); err == nil {
		t.Fatal("expected an empty allowlist to disable command execution")
	}
}

func TestParseCommandRejectsExecutableOutsideAllowlist(t *testing.T) {
	t.Setenv("TASKGRID_ALLOWED_COMMANDS", "echo,sleep")

	if _, _, err := parseCommand("uname -a"); err == nil {
		t.Fatal("expected an executable outside the allowlist to be rejected")
	}
}

func TestParseCommandRejectsBlankCommand(t *testing.T) {
	if _, _, err := parseCommand(" \t "); err == nil {
		t.Fatal("expected blank command to be rejected")
	}
}

func clearAuthEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("TASKGRID_API_TOKEN", "")
	t.Setenv("TASKGRID_ADMIN_TOKEN", "")
	t.Setenv("TASKGRID_SUBMIT_TOKEN", "")
	t.Setenv("TASKGRID_READ_TOKEN", "")
	t.Setenv("TASKGRID_REQUIRE_AUTH", "")
}

func TestAuthenticationFailsClosedByDefault(t *testing.T) {
	clearAuthEnvironment(t)

	if !authRequired() {
		t.Fatal("expected authentication to be required by default")
	}
	if err := validateAuthConfig(); err == nil {
		t.Fatal("expected startup validation to reject a missing token")
	}

	handler := withAPIProtection(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/jobs", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestAuthenticationCanBeExplicitlyDisabled(t *testing.T) {
	clearAuthEnvironment(t)
	t.Setenv("TASKGRID_REQUIRE_AUTH", "false")

	if authRequired() {
		t.Fatal("expected explicit false to disable authentication")
	}
	if err := validateAuthConfig(); err != nil {
		t.Fatalf("validateAuthConfig returned an error: %v", err)
	}

	handler := withAPIProtection(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/jobs", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestValidateAuthConfigRejectsInvalidRequirement(t *testing.T) {
	clearAuthEnvironment(t)
	t.Setenv("TASKGRID_REQUIRE_AUTH", "sometimes")
	t.Setenv("TASKGRID_API_TOKEN", "test-token")

	if err := validateAuthConfig(); err == nil {
		t.Fatal("expected an invalid TASKGRID_REQUIRE_AUTH value to be rejected")
	}
}

func TestAPIProtectionRequiresBearerToken(t *testing.T) {
	t.Setenv("TASKGRID_API_TOKEN", "test-token")
	handler := withAPIProtection(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/jobs", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	handler.ServeHTTP(authorized, req)
	if authorized.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", authorized.Code, http.StatusNoContent)
	}
	if authorized.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected request ID response header")
	}
}

func TestAPIProtectionLeavesHealthUnauthenticated(t *testing.T) {
	t.Setenv("TASKGRID_API_TOKEN", "test-token")
	handler := withAPIProtection(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestScopedTokensEnforceRoles(t *testing.T) {
	t.Setenv("TASKGRID_API_TOKEN", "")
	t.Setenv("TASKGRID_ADMIN_TOKEN", "admin-token")
	t.Setenv("TASKGRID_SUBMIT_TOKEN", "submit-token")
	t.Setenv("TASKGRID_READ_TOKEN", "read-token")

	handler := withAPIProtection(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name       string
		method     string
		path       string
		token      string
		wantStatus int
	}{
		{"admin can submit", http.MethodPost, "/jobs", "admin-token", http.StatusNoContent},
		{"admin can read", http.MethodGet, "/jobs", "admin-token", http.StatusNoContent},
		{"submit can submit", http.MethodPost, "/jobs", "submit-token", http.StatusNoContent},
		{"submit cannot read", http.MethodGet, "/jobs", "submit-token", http.StatusForbidden},
		{"read can read", http.MethodGet, "/jobs/job-1", "read-token", http.StatusNoContent},
		{"read cannot submit", http.MethodPost, "/jobs", "read-token", http.StatusForbidden},
		{"unknown token rejected", http.MethodGet, "/jobs", "wrong", http.StatusUnauthorized},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", "Bearer "+test.token)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
		})
	}
}

func TestValidateAuthConfigRejectsDuplicateTokens(t *testing.T) {
	t.Setenv("TASKGRID_API_TOKEN", "")
	t.Setenv("TASKGRID_ADMIN_TOKEN", "same-token")
	t.Setenv("TASKGRID_SUBMIT_TOKEN", "same-token")
	t.Setenv("TASKGRID_READ_TOKEN", "")
	if err := validateAuthConfig(); err == nil {
		t.Fatal("expected duplicate scoped tokens to be rejected")
	}
}

func TestRateLimitCanBeDisabled(t *testing.T) {
	t.Setenv("TASKGRID_RATE_LIMIT_PER_MINUTE", "0")
	allowed, err := withinRateLimit(t.Context(), "test-token")
	if err != nil {
		t.Fatalf("withinRateLimit returned an error: %v", err)
	}
	if !allowed {
		t.Fatal("expected disabled rate limiter to allow the request")
	}
}

func TestRuleSchedulerClassifiesWorkloads(t *testing.T) {
	tests := []struct {
		description string
		want        string
	}{
		{"compile a large source tree", "cpu"},
		{"download a backup from storage", "io"},
		{"send a lightweight notification", "general"},
	}

	for _, test := range tests {
		decision := scheduleWithRules(ScheduleRequest{Description: test.description})
		if decision.WorkloadType != test.want {
			t.Fatalf("description %q classified as %q, want %q", test.description, decision.WorkloadType, test.want)
		}
	}
}

func TestRuleSchedulerHonorsExplicitWorkloadHint(t *testing.T) {
	decision := scheduleWithRules(ScheduleRequest{
		Description: "[workload:general] benchmark delay",
		Command:     "sleep 2",
	})
	if decision.WorkloadType != "general" {
		t.Fatalf("explicit general hint classified as %q", decision.WorkloadType)
	}
}

func TestLimitedBufferBoundsOutput(t *testing.T) {
	buffer := &limitedBuffer{limit: 5}
	written, err := buffer.Write([]byte("123456789"))
	if err != nil {
		t.Fatalf("Write returned an error: %v", err)
	}
	if written != 9 {
		t.Fatalf("Write reported %d bytes, want 9", written)
	}
	if got := buffer.String(); got != "12345" {
		t.Fatalf("buffer = %q, want %q", got, "12345")
	}
	if !buffer.truncated {
		t.Fatal("expected output to be marked truncated")
	}
}

func TestDecodeGroqResponseBoundsInput(t *testing.T) {
	valid := `{"choices":[{"message":{"content":"{}"}}]}`
	response, err := decodeGroqResponse(strings.NewReader(valid))
	if err != nil {
		t.Fatalf("decodeGroqResponse returned an error: %v", err)
	}
	if len(response.Choices) != 1 {
		t.Fatalf("choices = %d, want 1", len(response.Choices))
	}

	oversized := strings.NewReader(strings.Repeat("x", maxGroqResponseBytes+1))
	if _, err := decodeGroqResponse(oversized); err == nil {
		t.Fatal("expected an oversized Groq response to be rejected")
	}
}

func TestClaimScriptsVerifyOwnershipBeforeMutation(t *testing.T) {
	for name, script := range map[string]string{
		"mark running": persistClaimedJobLua,
		"final move":   persistAndMoveLua,
	} {
		t.Run(name, func(t *testing.T) {
			claimLookup := strings.Index(script, `redis.call("LPOS"`)
			claimGuard := strings.Index(script, "if claim_position == false")
			stateWrite := strings.Index(script, `redis.call("SET"`)
			if claimLookup < 0 || claimGuard < 0 || stateWrite < 0 {
				t.Fatal("claim script is missing its lookup, guard, or state write")
			}
			if !(claimLookup < claimGuard && claimGuard < stateWrite) {
				t.Fatal("claim ownership must be verified before job state is written")
			}
		})
	}
}

func TestClaimTransitionsRejectLostOwnership(t *testing.T) {
	redisAddress := strings.TrimSpace(os.Getenv("TASKGRID_TEST_REDIS_ADDR"))
	if redisAddress == "" {
		t.Skip("TASKGRID_TEST_REDIS_ADDR is not configured")
	}

	testClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	if err := testClient.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("test Redis is unavailable: %v", err)
	}
	defer testClient.Close()

	previousClient := rdb
	rdb = testClient
	defer func() { rdb = previousClient }()

	testID := fmt.Sprintf("claim-test-%d", time.Now().UnixNano())
	processingQueue := processingPrefix + testID
	targetQueue := "taskgrid:test:target:" + testID
	metricKey := "taskgrid:test:metric:" + testID
	jobKey := jobKeyPrefix + testID
	defer testClient.Del(t.Context(), processingQueue, targetQueue, metricKey, jobKey)

	initial := Job{
		ID:           testID,
		Status:       "QUEUED",
		WorkloadType: "general",
		MaxAttempts:  3,
		CreatedAt:    time.Now().UTC(),
	}
	initialData, err := json.Marshal(initial)
	if err != nil {
		t.Fatalf("marshal initial job: %v", err)
	}
	if err := testClient.Set(t.Context(), jobKey, initialData, time.Minute).Err(); err != nil {
		t.Fatalf("seed initial job: %v", err)
	}

	running := initial
	running.Status = "RUNNING"
	running.WorkerID = testID
	if err := persistClaimedJob(running, processingQueue); err == nil {
		t.Fatal("expected RUNNING transition without a claim to be rejected")
	}
	stored, err := loadJob(testID)
	if err != nil {
		t.Fatalf("load job after rejected RUNNING transition: %v", err)
	}
	if stored.Status != initial.Status {
		t.Fatalf("rejected RUNNING transition changed status to %q", stored.Status)
	}

	if err := testClient.RPush(t.Context(), processingQueue, testID).Err(); err != nil {
		t.Fatalf("seed processing claim: %v", err)
	}
	if err := persistClaimedJob(running, processingQueue); err != nil {
		t.Fatalf("persist owned RUNNING transition: %v", err)
	}

	if err := testClient.LRem(t.Context(), processingQueue, 1, testID).Err(); err != nil {
		t.Fatalf("remove processing claim: %v", err)
	}
	succeeded := running
	succeeded.Status = "SUCCEEDED"
	if err := persistAndMove(succeeded, processingQueue, targetQueue, metricKey); err == nil {
		t.Fatal("expected final transition after claim loss to be rejected")
	}
	stored, err = loadJob(testID)
	if err != nil {
		t.Fatalf("load job after rejected final transition: %v", err)
	}
	if stored.Status != running.Status {
		t.Fatalf("rejected final transition changed status to %q", stored.Status)
	}

	if err := testClient.RPush(t.Context(), processingQueue, testID).Err(); err != nil {
		t.Fatalf("restore processing claim: %v", err)
	}
	if err := persistAndMove(succeeded, processingQueue, targetQueue, metricKey); err != nil {
		t.Fatalf("complete owned final transition: %v", err)
	}
	stored, err = loadJob(testID)
	if err != nil {
		t.Fatalf("load completed job: %v", err)
	}
	if stored.Status != succeeded.Status {
		t.Fatalf("completed status = %q, want %q", stored.Status, succeeded.Status)
	}
	if length := testClient.LLen(t.Context(), processingQueue).Val(); length != 0 {
		t.Fatalf("processing queue length = %d, want 0", length)
	}
	if queuedID := testClient.LIndex(t.Context(), targetQueue, 0).Val(); queuedID != testID {
		t.Fatalf("target queue contains %q, want %q", queuedID, testID)
	}
	if count := testClient.Get(t.Context(), metricKey).Val(); count != "1" {
		t.Fatalf("transition metric = %q, want 1", count)
	}
}

func TestDecodeJSONRequestRejectsTrailingObject(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"name":"one"}{"name":"two"}`))
	response := httptest.NewRecorder()
	var payload map[string]string
	if decodeJSONRequest(response, request, &payload) {
		t.Fatal("expected a trailing JSON object to be rejected")
	}
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
