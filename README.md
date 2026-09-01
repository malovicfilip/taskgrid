# TaskGrid

TaskGrid is a Go-based distributed job scheduler. It uses Redis for durable job records and queueing, Groq for workload classification, and Kubernetes/KEDA to run and scale dedicated CPU, I/O, and general worker pools.

## Highlights

- AI-assisted routing to `cpu`, `io`, or `general` Redis queues
- Atomic Redis job claims, worker heartbeats, and abandoned-job recovery
- Separate Kubernetes worker pools with KEDA queue-based autoscaling
- Bearer-token API protection when `TASKGRID_API_TOKEN` is configured
- Health (`/health`), readiness (`/readyz`), and Prometheus-style metrics (`/metrics`) endpoints
- 30-second job timeout and graceful API shutdown
- Secure-by-default execution: jobs run without a shell unless explicitly enabled

## Quick start with Docker Compose

1. Create a local `.env` file (it is ignored by Git):

   ```dotenv
   GROQ_API_KEY=your-local-key
   GROQ_MODEL=openai/gpt-oss-20b
   TASKGRID_API_TOKEN=choose-a-long-random-token
   ```

2. Start the stack:

   ```powershell
   docker compose up --build
   ```

3. Call the API with the token:

   ```powershell
   $headers = @{ Authorization = 'Bearer your-token' }
   Invoke-RestMethod http://localhost:8080/health
   Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs -Headers $headers -ContentType application/json -Body '{"name":"hello","description":"verification","command":"echo taskgrid-ok"}'
   ```

## Kubernetes deployment

The repository uses a local kind cluster. Install Docker Desktop, Go 1.27, kubectl, kind, and Helm; then build and load the image:

```powershell
docker build -t taskgrid:latest .
kind create cluster --name taskgrid
kind load docker-image taskgrid:latest --name taskgrid
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

Create the Kubernetes Secret locally. Do not commit secrets or paste them into chat:

```powershell
kubectl create secret generic taskgrid-ai --from-literal=GROQ_API_KEY='your-local-key' --from-literal=TASKGRID_API_TOKEN='choose-a-long-random-token'
```

Deploy the intended manifests (do not additionally apply `worker.yaml`, `worker-hpa.yaml`, or `worker-scaledobjects.yaml`; those are legacy generic-worker experiments):

```powershell
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker-pools.yaml
kubectl port-forward service/taskgrid-api 8080:8080
```

## Environment variables

| Variable | Required | Purpose |
| --- | --- | --- |
| `REDIS_ADDR` | No | Redis address; defaults to `localhost:6379`. |
| `TASKGRID_MODE` | No | `api`, `worker`, or `all` (local development default). |
| `GROQ_API_KEY` | For AI scheduling | Groq API credential. Jobs use a general-queue fallback if Groq is unavailable. |
| `GROQ_MODEL` | No | Defaults to `openai/gpt-oss-20b`. |
| `TASKGRID_API_TOKEN` | Strongly recommended | Enables `Authorization: Bearer <token>` on application endpoints. |
| `TASKGRID_ALLOW_SHELL_COMMANDS` | No | Defaults to `false`. Set to `true` only in a trusted development environment. |

## Command-execution security

By default, TaskGrid executes commands directly, without `bash -lc`. This prevents shell interpolation and chaining. Commands are whitespace-split, so use simple executable-and-argument commands such as `echo taskgrid-ok`.

`TASKGRID_ALLOW_SHELL_COMMANDS=true` restores shell behavior for development compatibility, but must not be enabled for untrusted job submitters. TaskGrid is intentionally not safe to expose publicly without stronger identity, authorization, auditing, and sandboxing.

## Development

```powershell
go test ./...
go build ./...
```

GitHub Actions runs the test suite, compiles the application, and builds the container image on pushes and pull requests.
