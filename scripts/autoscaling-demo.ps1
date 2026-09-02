param(
    [int]$Jobs = 120,
    [int]$Concurrency = 20,
    [string]$ApiUrl = "http://localhost:8080",
    [string]$ResultsPath = ""
)

$ErrorActionPreference = "Stop"
$taskGridRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($ResultsPath)) {
    $ResultsPath = Join-Path $taskGridRoot "artifacts\autoscaling-result.json"
}

$tokenPresent = -not [string]::IsNullOrWhiteSpace($env:TASKGRID_ADMIN_TOKEN) -or
    -not [string]::IsNullOrWhiteSpace($env:TASKGRID_API_TOKEN)
if (-not $tokenPresent) {
    throw "Set TASKGRID_ADMIN_TOKEN or TASKGRID_API_TOKEN in this terminal before running the demo."
}

$resultsDirectory = Split-Path -Parent $ResultsPath
New-Item -ItemType Directory -Path $resultsDirectory -Force | Out-Null
$resolvedRoot = (Resolve-Path $taskGridRoot).Path
$resolvedResultsDirectory = (Resolve-Path $resultsDirectory).Path
if (-not $resolvedResultsDirectory.StartsWith($resolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "ResultsPath must remain inside the TaskGrid workspace."
}

$portForward = $null
$loadJob = $null
$peaks = @{
    cpu = 0
    io = 0
    general = 0
}

try {
    kubectl set env deployment/taskgrid-api TASKGRID_SCHEDULER_MODE=rules TASKGRID_RATE_LIMIT_PER_MINUTE=100000 | Out-Host
    kubectl rollout status deployment/taskgrid-api --timeout=120s | Out-Host

    try {
        Invoke-WebRequest -UseBasicParsing -Uri ($ApiUrl + "/readyz") -TimeoutSec 3 | Out-Null
    }
    catch {
        $portForward = Start-Process -FilePath "kubectl" -ArgumentList @(
            "port-forward",
            "service/taskgrid-api",
            "8080:8080"
        ) -WindowStyle Hidden -PassThru

        $deadline = (Get-Date).AddMinutes(1)
        $apiReady = $false
        do {
            try {
                Invoke-WebRequest -UseBasicParsing -Uri ($ApiUrl + "/readyz") -TimeoutSec 3 | Out-Null
                $apiReady = $true
                break
            }
            catch {
                Start-Sleep -Seconds 2
            }
        } while ((Get-Date) -lt $deadline)
        if (-not $apiReady) {
            throw "TaskGrid API did not become reachable through port-forwarding."
        }
    }

    $loadJob = Start-Job -ScriptBlock {
        param($Root, $Count, $Workers, $URL)
        Set-Location -LiteralPath $Root
        & go run ./cmd/loadtest --url $URL --jobs $Count --concurrency $Workers --wait=true
        if ($LASTEXITCODE -ne 0) {
            throw "Load-test client exited with code $LASTEXITCODE"
        }
    } -ArgumentList $taskGridRoot, $Jobs, $Concurrency, $ApiUrl

    while ($loadJob.State -in @("NotStarted", "Running")) {
        $deployments = kubectl get deployment taskgrid-worker-cpu taskgrid-worker-io taskgrid-worker-general -o json | ConvertFrom-Json
        foreach ($deployment in $deployments.items) {
            $pool = $deployment.metadata.name -replace "taskgrid-worker-", ""
            $desired = [int]$deployment.spec.replicas
            if ($desired -gt $peaks[$pool]) {
                $peaks[$pool] = $desired
            }
        }

        $deployments.items | Select-Object @{
            Name = "Pool"
            Expression = { $_.metadata.name }
        }, @{
            Name = "Desired"
            Expression = { $_.spec.replicas }
        }, @{
            Name = "Ready"
            Expression = { if ($null -eq $_.status.readyReplicas) { 0 } else { $_.status.readyReplicas } }
        } | Format-Table | Out-Host

        Start-Sleep -Seconds 2
        $loadJob = Get-Job -Id $loadJob.Id
    }

    if ($loadJob.State -ne "Completed") {
        Receive-Job -Job $loadJob | Out-Host
        throw "Load test did not complete successfully. State: $($loadJob.State)"
    }

    $loadOutput = @(Receive-Job -Job $loadJob)
    $loadText = ($loadOutput | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    $loadSummary = $loadText | ConvertFrom-Json

    $scaleDownStarted = Get-Date
    $scaleDownDeadline = $scaleDownStarted.AddMinutes(3)
    do {
        $deployments = kubectl get deployment taskgrid-worker-cpu taskgrid-worker-io taskgrid-worker-general -o json | ConvertFrom-Json
        $desiredTotal = ($deployments.items | ForEach-Object { [int]$_.spec.replicas } | Measure-Object -Sum).Sum
        if ($desiredTotal -eq 0) {
            break
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $scaleDownDeadline)

    if ($desiredTotal -ne 0) {
        throw "Worker pools did not scale back to zero within three minutes."
    }

    $evidence = [ordered]@{
        date_utc = (Get-Date).ToUniversalTime().ToString("o")
        cluster = (kubectl config current-context)
        jobs = $Jobs
        concurrency = $Concurrency
        submitted = $loadSummary.submitted
        submission_errors = $loadSummary.submission_errors
        succeeded = $loadSummary.succeeded
        failed = $loadSummary.failed
        unfinished = $loadSummary.unfinished
        submission_p50_ms = $loadSummary.submission_p50_ms
        submission_p95_ms = $loadSummary.submission_p95_ms
        submission_p99_ms = $loadSummary.submission_p99_ms
        elapsed_seconds = $loadSummary.elapsed_seconds
        peak_worker_replicas = $peaks
        scale_down_seconds = [math]::Round(((Get-Date) - $scaleDownStarted).TotalSeconds, 2)
    }

    $evidenceJson = $evidence | ConvertTo-Json -Depth 5
    Set-Content -LiteralPath $ResultsPath -Value $evidenceJson -Encoding UTF8
    $evidenceJson | Out-Host
    Write-Output ("Benchmark evidence saved to " + (Resolve-Path $ResultsPath).Path)
}
finally {
    if ($null -ne $loadJob) {
        Remove-Job -Job $loadJob -Force -ErrorAction SilentlyContinue
    }
    kubectl set env deployment/taskgrid-api TASKGRID_SCHEDULER_MODE=hybrid TASKGRID_RATE_LIMIT_PER_MINUTE=600 | Out-Host
    if ($null -ne $portForward -and -not $portForward.HasExited) {
        Stop-Process -Id $portForward.Id -Force
    }
}
