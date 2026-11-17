# Starts the SocialScope stack, retrying image pulls and builds until they succeed.
# Useful on unreliable connections: Docker keeps every layer that finished, so each
# retry resumes where the last one stopped.
#
#   ./scripts/docker_up.ps1                # pull + build + start
#   ./scripts/docker_up.ps1 -MaxAttempts 30
param([int]$MaxAttempts = 15)
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

function Retry($label, [string[]]$cmd) {
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        Write-Host "`n==> $label (attempt $i/$MaxAttempts)" -ForegroundColor Cyan
        & docker @cmd
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds ([Math]::Min(60, 10 * $i))
    }
    throw "$label kept failing - check your internet connection and run this script again"
}

docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "Docker is not running - start Docker Desktop first" }

Retry "pulling images" @("compose", "pull", "--ignore-buildable")
Retry "building SocialScope images" @("compose", "build")
Retry "starting the stack" @("compose", "up", "-d")

Write-Host "`n==> stack is up. Waiting for services to settle..." -ForegroundColor Green
Start-Sleep -Seconds 30
docker compose ps
Write-Host "`nNext: ./scripts/docker_pipeline.ps1   then open http://localhost:8000"
