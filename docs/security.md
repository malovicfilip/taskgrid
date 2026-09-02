# Security model

TaskGrid executes operating-system commands, so its trust boundary is narrower
than a typical CRUD API. The default deployment is designed for controlled
workloads, not arbitrary public users.

## Implemented controls

- Constant-time bearer-token comparison.
- Separate admin, submit-only, and read-only tokens, with legacy admin-token
  compatibility.
- Fail-closed authentication by default. Only an explicit
  TASKGRID_REQUIRE_AUTH=false permits startup without a token.
- Direct process execution uses a required executable allowlist. An empty
  TASKGRID_ALLOWED_COMMANDS value disables command execution.
- Shell evaluation is a separate opt-in and also requires bash to be explicitly
  present in TASKGRID_ALLOWED_COMMANDS.
- Request-body and field-size limits.
- Redis-backed per-credential rate limiting.
- Execution timeout and bounded captured output.
- Non-root containers, read-only root filesystems, dropped Linux capabilities,
  no privilege escalation, and RuntimeDefault seccomp in Kubernetes.
- Redis-only network policy for production-capable CNIs.
- Secrets are referenced from Kubernetes Secrets and never stored in manifests.
- Structured audit logs include request ID, role, route, status, and latency.

## Remaining trust assumptions

- An allowed executable can still contain a vulnerability or perform unwanted
  network actions.
- Bearer tokens are service credentials, not end-user identity. Internet-facing
  use should add an identity provider, short-lived tokens, TLS termination, and
  per-user authorization.
- Redis is isolated by Kubernetes networking but is not configured with TLS.
  A managed Redis service with TLS and authentication is recommended in cloud
  deployments.
- Jobs use at-least-once delivery. External side effects must be idempotent.
- Workers are not a sandbox for arbitrary untrusted code. Non-root containers,
  seccomp, dropped capabilities, timeouts, and an allowlist reduce risk but do
  not make a hostile executable safe. Only trusted callers may submit jobs, and
  shell mode must remain disabled for untrusted submissions.

## AI data disclosure

In `ai` and `hybrid` scheduler modes, TaskGrid sends the submitted job name,
description, and command over HTTPS to the configured Groq model for workload
classification. Groq responses are capped at 64 KiB before JSON parsing. Use
`TASKGRID_SCHEDULER_MODE=rules` when job metadata must not leave the deployment;
rules mode makes no Groq request.

## Public operational endpoints

`/health`, `/readyz`, and `/metrics` are intentionally unauthenticated so
Kubernetes probes and Prometheus can reach them without a TaskGrid bearer token.
They do not return job payloads, but readiness and aggregate metrics still
reveal operational information. Internet-facing deployments must restrict these
paths using an ingress, firewall, or private monitoring network.

## Secret handling

Keep local values in ignored .env files. In Kubernetes, use an external secret
manager or create taskgrid-ai out-of-band. Rotate one scoped token at a time,
restart the API, verify clients, and then revoke the old token.
