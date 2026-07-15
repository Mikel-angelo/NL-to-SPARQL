Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

$Timestamp = Get-Date -Format "yyyyMMdd-HHmm"
$RunLabel = "overnight-compact-uri-label-$Timestamp"
$LogRoot = Join-Path $Root "overnight_runs\$RunLabel"
$PackagesRoot = Join-Path $Root "ontology_packages"
$KgRoot = Join-Path $Root "resources\knowledge_graphs"
$DatasetRoot = Join-Path $Root "evaluation\datasets"
$Python = Join-Path $Root ".venv\Scripts\python.exe"

New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null
Start-Transcript -Path (Join-Path $LogRoot "transcript.log") -Append | Out-Null

if (-not (Test-Path $Python)) {
    throw "Python executable not found: $Python"
}

function Invoke-Native {
    param(
        [Parameter(Mandatory=$true)][string]$StepName,
        [Parameter(Mandatory=$true)][string]$Exe,
        [Parameter(Mandatory=$true)][string[]]$Arguments
    )

    $safeName = ($StepName -replace '[^\w.-]+', '_')
    $logPath = Join-Path $LogRoot "$safeName.log"
    $stdoutPath = Join-Path $LogRoot "$safeName.stdout.log"
    $stderrPath = Join-Path $LogRoot "$safeName.stderr.log"
    Write-Host ""
    Write-Host "===== $StepName ====="
    Write-Host "$Exe $($Arguments -join ' ')"

    $process = Start-Process `
        -FilePath $Exe `
        -ArgumentList $Arguments `
        -NoNewWindow `
        -Wait `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath

    $combined = @()
    if (Test-Path $stdoutPath) {
        $combined += Get-Content $stdoutPath
    }
    if (Test-Path $stderrPath) {
        $combined += Get-Content $stderrPath
    }
    $combined | Set-Content -Path $logPath
    $combined | Select-Object -Last 40 | ForEach-Object { Write-Host $_ }

    $exitCode = $process.ExitCode
    if ($exitCode -ne 0) {
        throw "$StepName failed with exit code $exitCode. See $logPath"
    }
}

function Assert-File {
    param([Parameter(Mandatory=$true)][string]$Path)
    if (-not (Test-Path $Path)) {
        throw "Required file not found: $Path"
    }
}

function Get-ActivePackagePath {
    $activeFile = Join-Path $PackagesRoot ".active_package"
    if (-not (Test-Path $activeFile)) {
        throw "Active package marker not found after onboarding: $activeFile"
    }
    $activePackage = (Get-Content $activeFile -Raw).Trim()
    if (-not (Test-Path $activePackage)) {
        throw "Active package path from marker does not exist: $activePackage"
    }
    return $activePackage
}

function Run-One {
    param(
        [Parameter(Mandatory=$true)][string]$Name,
        [Parameter(Mandatory=$true)][string]$Ontology,
        [Parameter(Mandatory=$true)][string]$Dataset
    )

    Assert-File $Ontology
    Assert-File $Dataset

    $started = Get-Date
    $status = "FAILED"
    $package = ""
    $errorMessage = ""

    try {
        Invoke-Native `
            -StepName "ONBOARD_$Name" `
            -Exe $Python `
            -Arguments @(
                "onboard.py",
                "--ontology", $Ontology,
                "--output", $PackagesRoot,
                "--name", "$Name-$RunLabel"
            )

        $package = Get-ActivePackagePath

        Invoke-Native `
            -StepName "ACTIVATE_$Name" `
            -Exe $Python `
            -Arguments @(
                "activate.py",
                "--package", $package
            )

        $package = Get-ActivePackagePath

        Invoke-Native `
            -StepName "EVALUATE_$Name" `
            -Exe $Python `
            -Arguments @(
                "evaluate.py",
                "--package", $package,
                "--dataset", $Dataset,
                "--k", "5",
                "--abox-rag",
                "--abox-k", "5"
            )

        $status = "OK"
    }
    catch {
        $errorMessage = $_.Exception.Message
        Write-Host "FAILED: $Name"
        Write-Host $errorMessage
    }

    return [pscustomobject]@{
        name = $Name
        status = $status
        package = $package
        ontology = $Ontology
        dataset = $Dataset
        started = $started.ToString("s")
        finished = (Get-Date).ToString("s")
        error = $errorMessage
    }
}

$jobs = @(
    [pscustomobject]@{
        Name = "enovation"
        Ontology = Join-Path $KgRoot "eNOVATION.ttl"
        Dataset = Join-Path $DatasetRoot "eNOVATION_eval_dataset.json"
    },
    [pscustomobject]@{
        Name = "ck25"
        Ontology = Join-Path $KgRoot "ck25_text2sparql_combined.ttl"
        Dataset = Join-Path $DatasetRoot "ck25_text2sparql_combined_eval_dataset.json"
    },
    [pscustomobject]@{
        Name = "bestiary"
        Ontology = Join-Path $KgRoot "bestiary_sandro.rdf"
        Dataset = Join-Path $DatasetRoot "bestiary_sandro_eval_dataset.json"
    }
)

$spiderNames = @(
    "spider_battle_death_combined",
    "spider_car_1_combined",
    "spider_concert_singer_combined",
    "spider_course_teach_combined",
    "spider_cre_doc_template_mgt_combined",
    "spider_dog_kennels_combined",
    "spider_employee_hire_evaluation_combined",
    "spider_flight_2_combined",
    "spider_museum_visit_combined",
    "spider_network_1_combined",
    "spider_pets_1_combined",
    "spider_poker_player_combined",
    "spider_real_estate_properties_combined",
    "spider_singer_combined",
    "spider_student_transcripts_tracking_combined",
    "spider_tvshow_combined",
    "spider_voter_1_combined",
    "spider_world_1_combined"
)

foreach ($spiderName in $spiderNames) {
    $jobs += [pscustomobject]@{
        Name = "sandro-$($spiderName -replace '^spider_', '' -replace '_combined$', '' -replace '_', '-')"
        Ontology = Join-Path $KgRoot "spider4sparql_sandro\$spiderName.ttl"
        Dataset = Join-Path $DatasetRoot "spider4sparql_sandro\$($spiderName)_eval_dataset.json"
    }
}

$results = New-Object System.Collections.Generic.List[object]
Write-Host "Run label: $RunLabel"
Write-Host "Project root: $Root"
Write-Host "Log root: $LogRoot"
Write-Host "Total jobs: $($jobs.Count)"

foreach ($job in $jobs) {
    $results.Add((Run-One -Name $job.Name -Ontology $job.Ontology -Dataset $job.Dataset))
}

$summaryPath = Join-Path $LogRoot "summary.csv"
$results | Export-Csv -NoTypeInformation -Path $summaryPath

Write-Host ""
Write-Host "===== SUMMARY ====="
$results | Format-Table name, status, package, error -AutoSize
Write-Host "Summary CSV: $summaryPath"
Write-Host "Transcript: $(Join-Path $LogRoot 'transcript.log')"

$failed = @($results | Where-Object { $_.status -ne "OK" })
Stop-Transcript | Out-Null

if ($failed.Count -gt 0) {
    throw "$($failed.Count) job(s) failed. Check $summaryPath and logs under $LogRoot"
}
