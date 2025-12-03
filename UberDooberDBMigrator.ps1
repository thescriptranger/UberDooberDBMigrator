<#
.SYNOPSIS
    UberDooberDBMigrator - A PowerShell-based database migration tool that maps data from various source databases to SQL Server/Azure SQL targets using XML configuration.
 
.NOTES
    Name: UberDooberDBMigrator
    Author: The Script Ranger
    Version: 1.0
    DateCreated: 2025.06.03
 
.EXAMPLE
    .\UberDooberDBMigrator.ps1
    Runs the migration with default settings (fresh start, Info log level).

.EXAMPLE
    .\UberDooberDBMigrator.ps1 -ValidateOnly
    Validates the configuration without executing the migration.

.EXAMPLE
    .\UberDooberDBMigrator.ps1 -Resume
    Resumes a previously interrupted migration from where it left off.

.EXAMPLE
    .\UberDooberDBMigrator.ps1 -TableFilter -LogLevel Verbose
    Runs only tables marked with tableFilter="true" with verbose logging.

.LINK
    https://github.com/YourRepositoryLinkHere
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false, HelpMessage = "Run validation without executing migration")]
    [switch] $ValidateOnly,

    [Parameter(Mandatory = $false, HelpMessage = "Resume from previous run")]
    [switch] $Resume,

    [Parameter(Mandatory = $false, HelpMessage = "Run only tables with tableFilter='true'")]
    [switch] $TableFilter,

    [Parameter(Mandatory = $false, HelpMessage = "Log level: Error, Warning, Info, Verbose, Debug")]
    [ValidateSet("Error", "Warning", "Info", "Verbose", "Debug")]
    [string] $LogLevel = "Info"
)

BEGIN {
    $stopWatch = [System.Diagnostics.Stopwatch]::StartNew()

    $myRootPath = $PSScriptRoot
    $myFunctionName = $MyInvocation.MyCommand.Name

    Push-Location $myRootPath

    # Create required directories if they don't exist
    $directories = @("_logs", "_output", "_validationoutput")
    foreach ($dir in $directories) {
        $dirPath = Join-Path -Path $myRootPath -ChildPath $dir
        if (-not (Test-Path $dirPath)) {
            New-Item -Path $dirPath -ItemType Directory -Force | Out-Null
        }
    }

    # Set up logging
    $logFile = Join-Path -Path "$myRootPath\_logs" -ChildPath "$((Get-Date).ToString('yyyy.MM.dd.HHmmss')).$myFunctionName.log"
    $configFile = Join-Path -Path $myRootPath -ChildPath "UberDooberDBMigrator.xml"

    Start-Transcript -Path $logFile

    # Load global configuration
    $globalConfig = $null
    if (Test-Path $configFile) {
        [xml]$settings = Get-Content $configFile
        $globalConfig = @{
            Environment     = $settings.configuration.variables.Environment
            DefaultLogLevel = $settings.configuration.variables.DefaultLogLevel
        }

        # Use default log level from config if not specified via parameter
        if (-not $PSBoundParameters.ContainsKey('LogLevel') -and $globalConfig.DefaultLogLevel) {
            $LogLevel = $globalConfig.DefaultLogLevel
        }
    }

    # Set script-level log level for Write-Log function
    $script:LogLevel = $LogLevel

    # Load child function scripts
    Get-ChildItem -Path "$myRootPath\_function" -Filter "*.ps1" -Recurse | ForEach-Object { . $_.FullName }

    Write-Log -Message "========================================" -Level Info
    Write-Log -Message "UberDooberDBMigrator Starting" -Level Info
    Write-Log -Message "Environment: $($globalConfig.Environment)" -Level Info
    Write-Log -Message "Log Level: $LogLevel" -Level Info
    Write-Log -Message "ValidateOnly: $ValidateOnly" -Level Info
    Write-Log -Message "Resume: $Resume" -Level Info
    Write-Log -Message "TableFilter: $TableFilter" -Level Info
    Write-Log -Message "========================================" -Level Info
}

PROCESS {
    try {
        # Load master configuration
        $masterConfigPath = Join-Path -Path $myRootPath -ChildPath "_migration\MasterConfig.xml"
        
        if (-not (Test-Path $masterConfigPath)) {
            throw "Master configuration file not found: $masterConfigPath"
        }

        Write-Log -Message "Loading master configuration..." -Level Info
        $masterConfig = Read-MasterConfig -Path $masterConfigPath

        # Set default values if not specified
        if ($null -eq $masterConfig.BatchSize) { $masterConfig.BatchSize = 0 }
        if ($null -eq $masterConfig.QueryTimeoutSeconds -or $masterConfig.QueryTimeoutSeconds -le 0) { 
            $masterConfig.QueryTimeoutSeconds = 300 
        }

        Write-Log -Message "Migration: $($masterConfig.MigrationName)" -Level Info
        Write-Log -Message "Batch Size: $($masterConfig.BatchSize)" -Level Info
        Write-Log -Message "Query Timeout: $($masterConfig.QueryTimeoutSeconds) seconds" -Level Info

        # Load table mappings for tables that have them
        $tableMappings = @()
        foreach ($table in $masterConfig.Tables) {
            if (-not [string]::IsNullOrWhiteSpace($table.File)) {
                $tableMappingPath = Join-Path -Path $myRootPath -ChildPath "_migration\_tablemappings\$($table.File)"
                
                if (Test-Path $tableMappingPath) {
                    $tableMapping = Read-TableMapping -Path $tableMappingPath
                    $tableMappings += $tableMapping
                    Write-Log -Message "Loaded table mapping: $($table.File)" -Level Verbose
                } else {
                    Write-Log -Message "Table mapping file not found: $($table.File)" -Level Warning
                }
            }
        }

        # Generate migration run ID
        $migrationRunId = (Get-Date).ToString("yyyyMMdd_HHmmss")

        # Filter tables if -TableFilter is specified
        $tablesToProcess = $masterConfig.Tables
        if ($TableFilter) {
            $tablesToProcess = $masterConfig.Tables | Where-Object { $_.TableFilter -eq $true }
            Write-Log -Message "TableFilter applied: $($tablesToProcess.Count) tables selected" -Level Info
        }

        if ($tablesToProcess.Count -eq 0) {
            throw "No tables to process. Check your configuration or TableFilter setting."
        }

        # VALIDATION MODE
        if ($ValidateOnly) {
            Write-Log -Message "Running in VALIDATION mode..." -Level Info

            $validation = Test-MigrationConfiguration -MasterConfig $masterConfig -RootPath $myRootPath -TableMappings $tableMappings

            # Write validation output
            $validationFilePath = Get-MigrationOutputPath -RootPath $myRootPath -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId -OutputType "Validation" -IsValidation
            Write-ValidationOutput -FilePath $validationFilePath -ValidationData $validation

            Write-Log -Message "========================================" -Level Info
            Write-Log -Message "VALIDATION RESULTS" -Level Info
            Write-Log -Message "Valid: $($validation.isValid)" -Level Info
            Write-Log -Message "Tables Validated: $($validation.summary.tablesValidated)" -Level Info
            Write-Log -Message "Errors Found: $($validation.summary.errorsFound)" -Level Info
            Write-Log -Message "Warnings Found: $($validation.summary.warningsFound)" -Level Info
            Write-Log -Message "Output: $validationFilePath" -Level Info
            Write-Log -Message "========================================" -Level Info

            if (-not $validation.isValid) {
                throw "Validation failed with $($validation.summary.errorsFound) error(s). Review the validation output for details."
            }

            return
        }

        # MIGRATION MODE
        Write-Log -Message "Running in MIGRATION mode..." -Level Info

        # Initialize output files
        $progressFilePath = Get-MigrationOutputPath -RootPath $myRootPath -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId -OutputType "Progress"
        $rowErrorsFilePath = Get-MigrationOutputPath -RootPath $myRootPath -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId -OutputType "RowErrors"
        $errorLogFilePath = Get-MigrationOutputPath -RootPath $myRootPath -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId -OutputType "ErrorLog"

        # Handle Resume mode
        $progressData = $null
        $rowErrorsData = $null
        $errorLogData = $null

        if ($Resume) {
            # Find the most recent progress file
            $outputFolder = Join-Path -Path $myRootPath -ChildPath "_output"
            $existingProgressFiles = Get-ChildItem -Path $outputFolder -Filter "UberDooberDBMigrator_$($masterConfig.MigrationName)_*_Progress.json" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending

            if ($existingProgressFiles.Count -eq 0) {
                throw "Resume requested but no progress file found. Cannot resume without existing progress data."
            }

            $latestProgressFile = $existingProgressFiles[0].FullName
            Write-Log -Message "Resuming from: $latestProgressFile" -Level Info

            $progressData = Read-ProgressOutput -FilePath $latestProgressFile

            # Use the same run ID for resume
            if ($latestProgressFile -match "_(\d{8}_\d{6})_Progress\.json$") {
                $migrationRunId = $Matches[1]
            }

            # Update file paths to use the original run ID
            $progressFilePath = $latestProgressFile
            $rowErrorsFilePath = Get-MigrationOutputPath -RootPath $myRootPath -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId -OutputType "RowErrors"
            $errorLogFilePath = Get-MigrationOutputPath -RootPath $myRootPath -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId -OutputType "ErrorLog"

            # Load existing error files if they exist
            if (Test-Path $rowErrorsFilePath) {
                $rowErrorsJson = Get-Content -Path $rowErrorsFilePath -Raw
                $rowErrorsData = $rowErrorsJson | ConvertFrom-Json -AsHashtable
            }

            if (Test-Path $errorLogFilePath) {
                $errorLogJson = Get-Content -Path $errorLogFilePath -Raw
                $errorLogData = $errorLogJson | ConvertFrom-Json -AsHashtable
            }
        }

        # Initialize tracking objects if not resuming
        if ($null -eq $progressData) {
            $progressData = Initialize-ProgressOutput -MigrationName $masterConfig.MigrationName -Tables $tablesToProcess
            Write-ProgressOutput -FilePath $progressFilePath -ProgressData $progressData
        }

        if ($null -eq $rowErrorsData) {
            $rowErrorsData = Initialize-RowErrorsOutput -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId
        }

        if ($null -eq $errorLogData) {
            $errorLogData = Initialize-ErrorLogOutput -MigrationName $masterConfig.MigrationName -MigrationRunId $migrationRunId
        }

        # Connect to databases
        Write-Log -Message "Connecting to source database..." -Level Info
        $sourceConnection = Connect-Database -ConnectionConfig $masterConfig.SourceConnection -ConnectionName "Source" -TimeoutSeconds $masterConfig.QueryTimeoutSeconds

        Write-Log -Message "Connecting to target database..." -Level Info
        $targetConnection = Connect-Database -ConnectionConfig $masterConfig.TargetConnection -ConnectionName "Target" -TimeoutSeconds $masterConfig.QueryTimeoutSeconds

        try {
            # Disable constraints on target database
            Write-Log -Message "Disabling constraints on target database..." -Level Info
            Disable-DatabaseConstraints -Connection $targetConnection -TimeoutSeconds $masterConfig.QueryTimeoutSeconds

            # Clean up any existing key map tables from previous runs
            Write-Log -Message "Cleaning up any existing key map tables..." -Level Info
            Remove-AllKeyMapTables -Connection $targetConnection

            # Key maps for identity translation
            $keyMaps = @{}

            # Process each table
            foreach ($tableConfig in $tablesToProcess) {
                $sourceTableFull = "$($tableConfig.SourceSchema).$($tableConfig.SourceTable)"

                # Check if already completed (for resume)
                $tableProgress = $progressData.tables | Where-Object { $_.sourceTable -eq $sourceTableFull }
                if ($tableProgress -and $tableProgress.status -eq "Completed") {
                    Write-Log -Message "Skipping completed table: $sourceTableFull" -Level Info

                    # Load key mappings if this table generated them
                    $tableMapping = $tableMappings | Where-Object { 
                        $_.SourceSchema -eq $tableConfig.SourceSchema -and $_.SourceTable -eq $tableConfig.SourceTable 
                    } | Select-Object -First 1

                    if ($tableMapping -and $tableMapping.Settings.IdentityHandling -eq "GenerateNew") {
                        $keyMapTableName = Get-KeyMapTableName -SourceTable $sourceTableFull
                        $keyMapExists = $null
                        try {
                            $keyMaps[$sourceTableFull] = Get-AllKeyMappings -Connection $targetConnection -SourceTable $sourceTableFull
                        } catch {
                            Write-Log -Message "Could not load key mappings for $sourceTableFull (may not exist)" -Level Debug
                        }
                    }

                    continue
                }

                # Get table mapping if exists
                $tableMapping = $tableMappings | Where-Object { 
                    $_.SourceSchema -eq $tableConfig.SourceSchema -and $_.SourceTable -eq $tableConfig.SourceTable 
                } | Select-Object -First 1

                # Migrate the table
                Invoke-TableMigration -SourceConnection $sourceConnection -TargetConnection $targetConnection -TableConfig $tableConfig -TableMapping $tableMapping -MasterConfig $masterConfig -ProgressData $progressData -RowErrorsData $rowErrorsData -ErrorLogData $errorLogData -ProgressFilePath $progressFilePath -RowErrorsFilePath $rowErrorsFilePath -ErrorLogFilePath $errorLogFilePath -KeyMaps $keyMaps
            }

            # Re-enable constraints
            Write-Log -Message "Re-enabling constraints on target database..." -Level Info
            Enable-DatabaseConstraints -Connection $targetConnection -TimeoutSeconds $masterConfig.QueryTimeoutSeconds

            # Clean up key map tables
            Write-Log -Message "Cleaning up key map tables..." -Level Info
            Remove-AllKeyMapTables -Connection $targetConnection

            # Update final status
            $progressData.status = "Completed"
            Write-ProgressOutput -FilePath $progressFilePath -ProgressData $progressData

            # Write final error files
            Write-RowErrorsOutput -FilePath $rowErrorsFilePath -RowErrorsData $rowErrorsData
            Write-ErrorLogOutput -FilePath $errorLogFilePath -ErrorLogData $errorLogData

            Write-Log -Message "========================================" -Level Info
            Write-Log -Message "MIGRATION COMPLETED" -Level Info
            Write-Log -Message "Total Row Errors: $($rowErrorsData.totalRowErrors)" -Level Info
            Write-Log -Message "Total Error Log Entries: $($errorLogData.totalEntries)" -Level Info
            Write-Log -Message "Progress File: $progressFilePath" -Level Info
            Write-Log -Message "========================================" -Level Info
        }
        catch {
            # Update status to failed
            $progressData.status = "Failed"
            Write-ProgressOutput -FilePath $progressFilePath -ProgressData $progressData

            Add-ErrorLogEntry -ErrorLogData $errorLogData -Message "Migration failed: $($_.Exception.Message)"
            Write-ErrorLogOutput -FilePath $errorLogFilePath -ErrorLogData $errorLogData

            # Clean up key map tables even on failure
            Write-Log -Message "Cleaning up key map tables after failure..." -Level Info
            try {
                Remove-AllKeyMapTables -Connection $targetConnection
            } catch {
                Write-Log -Message "Failed to clean up key map tables: $($_.Exception.Message)" -Level Warning
            }

            # Re-enable constraints even on failure
            Write-Log -Message "Re-enabling constraints after failure..." -Level Info
            try {
                Enable-DatabaseConstraints -Connection $targetConnection -TimeoutSeconds $masterConfig.QueryTimeoutSeconds
            } catch {
                Write-Log -Message "Failed to re-enable constraints: $($_.Exception.Message)" -Level Warning
            }

            throw
        }
        finally {
            # Close connections
            if ($sourceConnection) {
                Disconnect-Database -Connection $sourceConnection -ConnectionName "Source"
            }
            if ($targetConnection) {
                Disconnect-Database -Connection $targetConnection -ConnectionName "Target"
            }
        }
    }
    catch {
        Write-Log -Message "FATAL ERROR: $($_.Exception.Message)" -Level Error -Exception $_.Exception
        throw
    }
}

END {
    $stopWatch.Stop()
    Write-Log -Message "========================================" -Level Info
    Write-Log -Message "UberDooberDBMigrator Completed" -Level Info
    Write-Log -Message "Elapsed Time: $($stopWatch.Elapsed)" -Level Info
    Write-Log -Message "========================================" -Level Info
    Stop-Transcript
    Pop-Location
}