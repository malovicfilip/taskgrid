# Security model

TaskGrid executes operating-system commands, so its trust boundary is narrower
than a typical CRUD API. The default deployment is designed for controlled
workloads, not arbitrary public users.

## Implemented controls

- Constant-time bearer-token comparison.
- Separate admin, submit-only, and read-only tokens, with legacy admin-token
  compatibility.
- Fail-closed production configuration through TASKGRID_REQUIRE_AUTH.
- Direct process execution by default; shell evaluation is opt-in.
- Optional executable allowlist.
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
- Shell mode must remain disabled for untrusted submissions.

## Secret handling

Keep local values in ignored .env files. In Kubernetes, use an external secret
manager or create taskgrid-ai out-of-band. Rotate one scoped token at a time,
restart the API, verify clients, and then revoke the old token.
