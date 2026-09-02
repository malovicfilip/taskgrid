# Autoscaling benchmark

The benchmark client submits an even mixture of CPU, I/O, and general jobs,
uses a unique idempotency key for every job, waits for terminal states, and
prints machine-readable p50, p95, and p99 submission latency.

Use rules-only scheduling during benchmarks. This makes the result repeatable
and prevents a load test from creating Groq API charges.

    $env:TASKGRID_API_TOKEN = "<your existing local token>"
    kubectl port-forward service/taskgrid-api 8080:8080
    .\scripts\autoscaling-demo.ps1 -Jobs 120 -Concurrency 20

Capture the terminal output showing each worker deployment scaling from zero,
the benchmark JSON summary, and the TaskGrid Grafana dashboard. Do not publish
the token or the contents of any secret.

## Results

Record measurements only after running the benchmark on a named environment.
Do not claim throughput or latency numbers that have not been reproduced.

### Verified local result

The following result was produced with rules-only scheduling on the
kind-taskgrid cluster running in Docker Desktop on a Windows laptop. Each pool
executed 40 two-second jobs. This is a reproducible local benchmark, not a
production SLA.

| Date (UTC) | Jobs | Concurrency | Success | p50 submit | p95 submit | p99 submit | Peak workers | Completion | Scale to zero |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-09-02 | 120 | 20 | 120/120 (100%) | 6.818 ms | 45.182 ms | 60.882 ms | 15 total (5/pool) | 29.18 s | 25.25 s |
