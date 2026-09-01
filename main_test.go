package main

import (
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
