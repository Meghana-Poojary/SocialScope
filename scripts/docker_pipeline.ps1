# Runs the full pipeline inside the docker-compose stack (nothing needed on Windows but Docker):
#   data -> Kafka -> Spark Structured Streaming -> HDFS -> Spark batch -> MongoDB / Elasticsearch / Neo4j
#
# Synthetic sample (default):
#   ./scripts/docker_pipeline.ps1
#   ./scripts/docker_pipeline.ps1 -Regenerate                  # new synthetic sample first
#
# Kaggle dataset - "Customer Support on Twitter" (real tweets to brands):
#   ./scripts/docker_pipeline.ps1 -Source kaggle               # downloads it if missing (needs a Kaggle API token)
#   ./scripts/docker_pipeline.ps1 -Source kaggle -Brands "AppleSupport,AmazonHelp,XboxSupport,AskPlayStation"
#   ./scripts/docker_pipeline.ps1 -Source kaggle -KaggleFile data/kaggle/twcs.csv   # a CSV you downloaded yourself
#
# Any other Kaggle CSV, via a column mapping:
#   ./scripts/docker_pipeline.ps1 -Source kaggle -KaggleFormat generic -KaggleFile data/kaggle/my.csv `
#       -Mapping data/kaggle/mapping.example.json -Brands "Nike,Adidas,Puma"
#
# Every run starts clean (Kafka topics recreated, HDFS data wiped) so datasets never mix; -Keep skips that.
param(
    [ValidateSet("synthetic", "kaggle")] [string]$Source = "synthetic",
    [switch]$Regenerate,
    [ValidateSet("twcs", "generic")] [string]$KaggleFormat = "twcs",
    [string]$KaggleFile = "",
    [string]$Dataset = "thoughtvector/customer-support-on-twitter",
    [string]$Brands = "",   # twcs default: AppleSupport,AmazonHelp,SpotifyCares,Uber_Support,XboxSupport,AskPlayStation
    [string]$Mapping = "",
    [int]$MaxPerBrand = 30000,
    [switch]$Keep
)
# "Continue", not "Stop": Spark logs to stderr, which Windows PowerShell 5.1 would
# otherwise treat as a fatal error. Failures are detected through exit codes instead.
$ErrorActionPreference = "Continue"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

function Step($label, [string[]]$cmd) {
    Write-Host "`n==> $label" -ForegroundColor Cyan
    # merge stderr into stdout and print as plain text (no red NativeCommandError noise)
    & docker @cmd 2>&1 | ForEach-Object { "$_" }
    if ($LASTEXITCODE -ne 0) { Write-Host "$label failed (exit $LASTEXITCODE)" -ForegroundColor Red; exit 1 }
}
function InContainer([string]$path) {
    # host path inside the project -> the same relative path inside the container (/app)
    $full = (Resolve-Path $path).Path
    if (-not $full.StartsWith($Root, [StringComparison]::OrdinalIgnoreCase)) {
        $dest = Join-Path $Root "data\kaggle\$(Split-Path $full -Leaf)"
        Write-Host "copying $full -> $dest (the containers can only see the project folder)"
        Copy-Item $full $dest -Force
        $full = $dest
    }
    return $full.Substring($Root.Length + 1).Replace("\", "/")
}

$exec = @("compose", "exec", "-T", "spark")
$submit = $exec + @("spark-submit", "--master", "local[2]", "--driver-memory", "1200m")
$produce = $exec + @("python3", "ingestion/producer.py")

if (-not $Keep) {
    Write-Host "`n==> starting clean: wiping /socialscope in HDFS (Kafka topics are recreated below)" -ForegroundColor Yellow
    docker compose exec -T namenode hdfs dfs -rm -r -f /socialscope 2>&1 | ForEach-Object { "$_" } | Where-Object { $_ -notmatch "WARN" }
    $produce += "--recreate-topics"
}

if ($Source -eq "synthetic") {
    if ($Regenerate -or -not (Test-Path "data/raw/posts.jsonl")) {
        Step "0/3 generating synthetic sample data" ($exec + @("python3", "ingestion/generate_sample.py"))
    }
    $produce += @("--posts", "data/raw/posts.jsonl", "--metrics", "data/raw/brand_metrics.jsonl")
}
else {
    # ---- locate or download the Kaggle CSV
    if (-not $KaggleFile) {
        if ($KaggleFormat -ne "twcs") { Write-Host "-KaggleFile is required for -KaggleFormat generic" -ForegroundColor Red; exit 1 }
        $found = Get-ChildItem -Path "data/kaggle" -Recurse -Filter "twcs.csv" -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $found) {
            # pass Kaggle credentials from the host (env vars or %USERPROFILE%\.kaggle\kaggle.json)
            $cfg = Join-Path $env:USERPROFILE ".kaggle\kaggle.json"
            if (-not $env:KAGGLE_USERNAME -and (Test-Path $cfg)) {
                $j = Get-Content $cfg -Raw | ConvertFrom-Json
                $env:KAGGLE_USERNAME = $j.username; $env:KAGGLE_KEY = $j.key
            }
            if (-not $env:KAGGLE_USERNAME) {
                Write-Host "`nNo Kaggle CSV found in data/kaggle and no Kaggle API token." -ForegroundColor Red
                Write-Host "Either: download twcs.csv from https://www.kaggle.com/datasets/$Dataset into data/kaggle/,"
                Write-Host "    or: create a token (kaggle.com -> Settings -> API -> Create New Token) and save it as $cfg"
                exit 1
            }
            Step "0/3 downloading $Dataset from Kaggle" ($exec[0..2] + @("-e", "KAGGLE_USERNAME", "-e", "KAGGLE_KEY", "spark",
                     "python3", "ingestion/kaggle_download.py", $Dataset))
            $found = Get-ChildItem -Path "data/kaggle" -Recurse -Filter "twcs.csv" | Select-Object -First 1
        }
        $KaggleFile = $found.FullName
    }
    if (-not $Brands -and $KaggleFormat -eq "twcs") { $Brands = "AppleSupport,AmazonHelp,SpotifyCares,Uber_Support,XboxSupport,AskPlayStation" }
    $csv = InContainer $KaggleFile
    $adapt = $exec + @("python3", "ingestion/kaggle_adapter.py", "--format", $KaggleFormat, "--input", $csv,
                       "--out", "data/raw/kaggle_posts.jsonl", "--max-per-brand", "$MaxPerBrand")
    if ($Brands) { $adapt += @("--brands", $Brands) }
    if ($Mapping) { $adapt += @("--mapping", (InContainer $Mapping)) }
    Step "0/3 converting $csv to the SocialScope format" $adapt
    $produce += @("--posts", "data/raw/kaggle_posts.jsonl", "--metrics", "none")   # no follower snapshots
}

Step "1/3 producing records to Kafka" $produce
Step "2/3 Spark Structured Streaming: Kafka -> HDFS" ($submit + @("processing/stream_to_hdfs.py", "--once"))
Step "3/3 Spark batch analytics: HDFS -> MongoDB / Elasticsearch / Neo4j" ($submit + @("processing/batch_analytics.py"))

Write-Host "`n==> done ($Source data). Dashboard: http://localhost:8000   API docs: http://localhost:8000/docs" -ForegroundColor Green
