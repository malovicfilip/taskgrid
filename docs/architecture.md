# Architecture

TaskGrid is an at-least-once distributed job system. It separates admission and
scheduling from execution so each workload class can scale independently.

~~~mermaid
flowchart LR
    C[API client] -->|Bearer token + optional idempotency key| A[Go API]
    A -->|AI mode| G[Groq]
    A -->|atomic job record + queue push| R[(Redis AOF)]
    R --> Q1[CPU queue]
    R --> Q2[I/O queue]
    R --> Q3[General queue]
    K[KEDA] -->|observes queue length| W1[CPU workers]
    K -->|observes queue length| W2[I/O workers]
    K -->|observes queue length| W3[General workers]
    Q1 --> W1
    Q2 --> W2
    Q3 --> W3
    W1 -->|status and output| R
    W2 -->|status and output| R
    W3 -->|status and output| R
    W1 -->|exhausted retries| D[Dead-letter queue]
    W2 -->|exhausted retries| D
    W3 -->|exhausted retries| D
    P[Prometheus] -->|scrape /metrics| A
    P --> GF[Grafana + alerts]
~~~

## Request lifecycle

1. The API authenticates the caller and enforces the caller's role.
2. An Idempotency-Key is resolved before scheduling, avoiding duplicate work and
   duplicate AI calls on client retries.
3. The hybrid scheduler asks Groq for a validated classification and falls back
   to deterministic rules if the provider is unavailable. Rules-only mode
   supports explicit workload hints for repeatable, no-cost benchmarks. AI and
   hybrid modes send the job name, description, and command to Groq.
4. One Redis Lua script writes the job, indexes it, pushes it to the selected
   queue, and records the idempotency mapping atomically.
5. A worker atomically moves the job from its queue to a worker-specific
   processing list before execution. Every later state transition verifies that
   processing-list claim before updating the stored job.
6. Heartbeats allow another worker to recover a claim after a worker failure.
7. Completion, retry, acknowledgement, and dead-letter transitions are performed
   with Redis scripts so job state and queue state do not drift apart.

## Delivery semantics

TaskGrid provides at-least-once execution. A worker can fail after an external
command has produced a side effect but before acknowledgement reaches Redis.
Submission idempotency prevents duplicate jobs, but commands that have external
side effects should also be idempotent. TaskGrid does not claim exactly-once
execution.

## Data model

Job records have a configurable retention period (seven days by default).
The ordered job index supports bounded pagination, while queue and global
completion counters remain in Redis. Redis uses append-only persistence in both
Compose and Kubernetes.
