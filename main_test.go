package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseCommand(t *testing.T) {
	t.Parallel()

	executable, args, err := parseCommand("echo taskgrid-ok")
	if err != nil {
		t.Fatalf("parseCommand returned an error: %v", err)
	}
	if executable != "echo" || len(args) != 1 || args[0] != "taskgrid-ok" {
		t.Fatalf("unexpected command: %q %#v", executable, args)
	}
}

func TestParseCommandRejectsBlankCommand(t *testing.T) {
	t.Parallel()

	if _, _, err := parseCommand(" \t "); err == nil {
		t.Fatal("expected blank command to be rejected")
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
