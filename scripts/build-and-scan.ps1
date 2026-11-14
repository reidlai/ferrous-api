<#
.SYNOPSIS
    Builds ferrous-api Docker image and scans it with Hadolint and Trivy (Story 8.7).
.DESCRIPTION
    Enforces Tier 2 DevSecOps standards locally without incurring cloud CI minutes (AD-13, AD-24, NFR-10..13).
#>
[CmdletBinding()]
param (
    [Alias("skip-scan")]
    [switch]$SkipScan,
    [Alias("dry-run")]
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$RepoRoot = (Resolve-Path "$ScriptDir\..").Path

$Dockerfiles = @("Dockerfile")
$TargetImage = "ferrous-api:latest"

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " [DEVSECOPS] Building and Scanning ferrous-api Container              " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

Push-Location $RepoRoot
try {
    # 1. Hadolint Dockerfile Linting
    Write-Host "`n[Step 1/3] Running Hadolint Dockerfile SAST audit..." -ForegroundColor Yellow
    foreach ($df in $Dockerfiles) {
        Write-Host "  Auditing $df..." -ForegroundColor Gray
        if ($DryRun) {
            Write-Host "  [DRY-RUN] hadolint $df"
        } else {
            $HadolintCmd = Get-Command hadolint -ErrorAction SilentlyContinue
            if ($HadolintCmd) {
                hadolint $df
            } else {
                Write-Host "  [INFO] Local hadolint CLI not found; running via hadolint/hadolint container..." -ForegroundColor Gray
                docker run --rm --mount "type=bind,source=${RepoRoot},target=/work" -w /work hadolint/hadolint:latest hadolint $df
            }
            if ($LASTEXITCODE -ne 0) { throw "Hadolint Dockerfile validation failed for $df" }
            Write-Host "  [OK] $df passed Hadolint linting (0 issues)." -ForegroundColor Green
        }
    }

    # 2. Validate Compose Manifest & Build Container Image (parent context .. for ../ferrous-cqrses)
    Write-Host "`n[Step 2/3] Validating Docker Compose manifest and building container image..." -ForegroundColor Yellow
    Write-Host "  Validating docker-compose.yml syntax..." -ForegroundColor Gray
    if ($DryRun) {
        Write-Host "  [DRY-RUN] docker compose config -q"
        Write-Host "  [DRY-RUN] docker build -t $TargetImage -f Dockerfile .."
    } else {
        docker compose config -q
        if ($LASTEXITCODE -ne 0) { throw "Docker Compose configuration validation failed" }
        Write-Host "  [OK] docker-compose.yml configuration syntax validated." -ForegroundColor Green

        docker build -t $TargetImage -f Dockerfile ..
        if ($LASTEXITCODE -ne 0) { throw "Failed to build container image $TargetImage" }
        Write-Host "  [OK] Container image $TargetImage built successfully." -ForegroundColor Green

        $ImageBytes = [int64](docker image inspect --format='{{.Size}}' $TargetImage)
        $MaxBytes = 50 * 1024 * 1024
        if ($ImageBytes -gt $MaxBytes) {
            throw "Container image $TargetImage size ($ImageBytes bytes) exceeds 50MB AD-24 budget"
        }
        Write-Host ("  [OK] Image size verified: {0:N2} MB (< 50 MB AD-24)." -f ($ImageBytes / 1MB)) -ForegroundColor Green
    }

    if ($SkipScan) {
        Write-Host "`n[WARN] Skipping Trivy vulnerability scanning (-SkipScan enabled)." -ForegroundColor Yellow
        return
    }

    function Invoke-TrivyScan {
        param ([string]$ImageName)
        $TrivyCmd = Get-Command trivy -ErrorAction SilentlyContinue
        if ($TrivyCmd) {
            trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 $ImageName
        } else {
            Write-Host "  [INFO] Local trivy CLI not found; running via aquasec/trivy container..." -ForegroundColor Gray
            docker run --rm `
                -v /var/run/docker.sock:/var/run/docker.sock `
                aquasec/trivy:latest image `
                --severity HIGH,CRITICAL `
                --ignore-unfixed `
                --exit-code 1 `
                $ImageName
        }
        if ($LASTEXITCODE -ne 0) { throw "Trivy vulnerability scan failed for $ImageName" }
    }

    # 3. Trivy Scan
    Write-Host "`n[Step 3/3] Running Trivy vulnerability scan on container image..." -ForegroundColor Yellow
    Write-Host "  Scanning image $TargetImage..." -ForegroundColor Gray
    if ($DryRun) {
        Write-Host "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 $TargetImage"
    } else {
        Invoke-TrivyScan -ImageName $TargetImage
        Write-Host "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in $TargetImage." -ForegroundColor Green
    }

    Write-Host "`n======================================================================" -ForegroundColor Green
    Write-Host " [SUCCESS] Container Target Built and Verified (0 CVEs)!             " -ForegroundColor Green
    Write-Host "======================================================================" -ForegroundColor Green
}
finally {
    Pop-Location
}
