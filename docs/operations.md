# Operations runbook

## Health and diagnosis

- /health proves the API process is alive and intentionally does not depend on
  Redis.
- /readyz proves the API can currently reach Redis.
- /metrics exposes request, latency, scheduling, retry, completion, dead-letter,
  and queue-depth metrics.
- These three operational endpoints intentionally bypass bearer authentication
  for Kubernetes and Prometheus. Restrict them at the network edge in any
  internet-facing deployment.
- API logs are JSON audit records for each request. Worker logs include job ID,
  pool, attempt, recovery, and final state.

Useful checks:

    kubectl get pods
    kubectl get scaledobjects
    kubectl get hpa
    kubectl logs deployment/taskgrid-api --tail=100
    kubectl logs deployment/taskgrid-worker-general --tail=100
    kubectl port-forward service/taskgrid-api 8080:8080

## Dead-letter response

1. Check taskgrid_queue_depth{queue="dead_letter"} and the worker logs.
2. Fetch the failed job by ID and inspect error, last_error, attempts, and
   output_truncated.
3. Correct the command or allowed-command configuration.
4. Submit a new job with a new idempotency key. The current API intentionally
   does not silently replay a dead-lettered command.

## Redis durability

Redis uses AOF with fsync every second and a persistent volume. This protects
against pod replacement, but it is not a complete disaster-recovery strategy.
For a real cloud deployment, use managed Redis backups or snapshot the
persistent volume and test restoration regularly.

## Deployment and rollback

Build immutable image tags for non-local environments instead of latest. Apply
the same tag to the API and all workers, wait for rollout status, then run a
small rules-mode smoke test. Roll back with kubectl rollout undo if readiness,
error-rate, or dead-letter alerts regress.

## Token rotation

TASKGRID_API_TOKEN remains a legacy admin credential. Prefer distinct
TASKGRID_ADMIN_TOKEN, TASKGRID_SUBMIT_TOKEN, and TASKGRID_READ_TOKEN values.
All configured token values must be different or the API refuses to start.
