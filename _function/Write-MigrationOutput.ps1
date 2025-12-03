Function Get-MigrationOutputPath {
    <#
    .SYNOPSIS
        Generates the output file path with consistent naming convention.
     
    .NOTES
        Name: Get-MigrationOutputPath
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $path = Get-MigrationOutputPath -RootPath "C:\Migration" -MigrationName "CustomerMigration" -MigrationRunId "20250603_143000" -OutputType "Progress"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $RootPath,

        [Parameter(Mandatory = $true)]
        [string] $MigrationName,

        [Parameter(Mandatory = $true)]
        [string] $MigrationRunId,

        [Parameter(Mandatory = $true)]
        [ValidateSet("Progress", "RowErrors", "ErrorLog", "Validation")]
        [string] $OutputType,

        [Parameter(Mandatory = $false)]
        [switch] $IsValidation
    )

    BEGIN {}

    PROCESS {
        $folder = if ($IsValidation) { "_validationoutput" } else { "_output" }
        $outputFolder = Join-Path -Path $RootPath -ChildPath $folder

        # Ensure folder exists
        if (-not (Test-Path $outputFolder)) {
            New-Item -Path $outputFolder -ItemType Directory -Force | Out-Null
        }

        $fileName = "UberDooberDBMigrator_$($MigrationName)_$($MigrationRunId)_$($OutputType).json"
        $filePath = Join-Path -Path $outputFolder -ChildPath $fileName

        return $filePath
    }

    END {}
}


Function Initialize-ProgressOutput {
    <#
    .SYNOPSIS
        Initializes a new progress tracking object.
     
    .NOTES
        Name: Initialize-ProgressOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $progress = Initialize-ProgressOutput -MigrationName "CustomerMigration" -Tables $tableList
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $MigrationName,

        [Parameter(Mandatory = $true)]
        [array] $Tables
    )

    BEGIN {}

    PROCESS {
        $tableProgress = @()
        foreach ($table in $Tables) {
            $tableProgress += @{
                sourceTable       = "$($table.SourceSchema).$($table.SourceTable)"
                targetTable       = "$($table.TargetSchema).$($table.TargetTable)"
                status            = "Pending"
                totalRows         = 0
                processedRows     = 0
                lastBatchKeyValue = $null
            }
        }

        $progress = @{
            migrationName = $MigrationName
            startedAt     = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            lastUpdatedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            status        = "InProgress"
            tables        = $tableProgress
        }

        return $progress
    }

    END {}
}


Function Write-ProgressOutput {
    <#
    .SYNOPSIS
        Writes or updates the progress JSON file.
     
    .NOTES
        Name: Write-ProgressOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Write-ProgressOutput -FilePath $progressFile -ProgressData $progress
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,

        [Parameter(Mandatory = $true)]
        [hashtable] $ProgressData
    )

    BEGIN {}

    PROCESS {
        $ProgressData.lastUpdatedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        $json = $ProgressData | ConvertTo-Json -Depth 10
        Set-Content -Path $FilePath -Value $json -Encoding UTF8

        Write-Log -Message "Progress file updated: $FilePath" -Level Debug
    }

    END {}
}


Function Read-ProgressOutput {
    <#
    .SYNOPSIS
        Reads an existing progress JSON file for resume functionality.
     
    .NOTES
        Name: Read-ProgressOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $progress = Read-ProgressOutput -FilePath $progressFile
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath
    )

    BEGIN {}

    PROCESS {
        if (-not (Test-Path $FilePath)) {
            throw "Progress file not found for resume: $FilePath"
        }

        $json = Get-Content -Path $FilePath -Raw
        $progress = $json | ConvertFrom-Json -AsHashtable

        Write-Log -Message "Progress file loaded for resume: $FilePath" -Level Info

        return $progress
    }

    END {}
}


Function Update-TableProgress {
    <#
    .SYNOPSIS
        Updates progress for a specific table.
     
    .NOTES
        Name: Update-TableProgress
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Update-TableProgress -ProgressData $progress -SourceTable "dbo.Customers" -Status "InProgress" -ProcessedRows 5000 -LastBatchKeyValue "5000"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $ProgressData,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $false)]
        [string] $Status,

        [Parameter(Mandatory = $false)]
        [int] $TotalRows,

        [Parameter(Mandatory = $false)]
        [int] $ProcessedRows,

        [Parameter(Mandatory = $false)]
        [string] $LastBatchKeyValue
    )

    BEGIN {}

    PROCESS {
        $tableEntry = $ProgressData.tables | Where-Object { $_.sourceTable -eq $SourceTable }

        if ($tableEntry) {
            if ($Status) { $tableEntry.status = $Status }
            if ($PSBoundParameters.ContainsKey('TotalRows')) { $tableEntry.totalRows = $TotalRows }
            if ($PSBoundParameters.ContainsKey('ProcessedRows')) { $tableEntry.processedRows = $ProcessedRows }
            if ($PSBoundParameters.ContainsKey('LastBatchKeyValue')) { $tableEntry.lastBatchKeyValue = $LastBatchKeyValue }
        }
    }

    END {}
}


Function Initialize-RowErrorsOutput {
    <#
    .SYNOPSIS
        Initializes a new row errors tracking object.
     
    .NOTES
        Name: Initialize-RowErrorsOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $rowErrors = Initialize-RowErrorsOutput -MigrationName "CustomerMigration" -MigrationRunId "20250603_143000"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $MigrationName,

        [Parameter(Mandatory = $true)]
        [string] $MigrationRunId
    )

    BEGIN {}

    PROCESS {
        $rowErrors = @{
            migrationName  = $MigrationName
            migrationRunId = $MigrationRunId
            generatedAt    = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            totalRowErrors = 0
            tables         = @()
        }

        return $rowErrors
    }

    END {}
}


Function Add-RowError {
    <#
    .SYNOPSIS
        Adds a row error to the row errors object.
     
    .NOTES
        Name: Add-RowError
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Add-RowError -RowErrorsData $rowErrors -SourceTable "dbo.Customers" -TargetTable "dbo.tblCustomers" -SourceKeyValue "123" -ErrorMessage "Truncation error" -SourceData $rowData
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $RowErrorsData,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $true)]
        [string] $TargetTable,

        [Parameter(Mandatory = $true)]
        [string] $SourceKeyValue,

        [Parameter(Mandatory = $true)]
        [string] $ErrorMessage,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceData
    )

    BEGIN {}

    PROCESS {
        # Find or create table entry
        $tableEntry = $RowErrorsData.tables | Where-Object { $_.sourceTable -eq $SourceTable }

        if (-not $tableEntry) {
            $tableEntry = @{
                sourceTable = $SourceTable
                targetTable = $TargetTable
                errorCount  = 0
                rows        = @()
            }
            $RowErrorsData.tables += $tableEntry
        }

        # Add row error
        $rowError = @{
            sourceKeyValue = $SourceKeyValue
            errorTimestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            errorMessage   = $ErrorMessage
            sourceData     = $SourceData
        }

        $tableEntry.rows += $rowError
        $tableEntry.errorCount++
        $RowErrorsData.totalRowErrors++
    }

    END {}
}


Function Write-RowErrorsOutput {
    <#
    .SYNOPSIS
        Writes the row errors JSON file.
     
    .NOTES
        Name: Write-RowErrorsOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Write-RowErrorsOutput -FilePath $rowErrorsFile -RowErrorsData $rowErrors
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,

        [Parameter(Mandatory = $true)]
        [hashtable] $RowErrorsData
    )

    BEGIN {}

    PROCESS {
        $RowErrorsData.generatedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        $json = $RowErrorsData | ConvertTo-Json -Depth 10
        Set-Content -Path $FilePath -Value $json -Encoding UTF8

        Write-Log -Message "Row errors file updated: $FilePath (Total errors: $($RowErrorsData.totalRowErrors))" -Level Debug
    }

    END {}
}


Function Initialize-ErrorLogOutput {
    <#
    .SYNOPSIS
        Initializes a new error log object.
     
    .NOTES
        Name: Initialize-ErrorLogOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $errorLog = Initialize-ErrorLogOutput -MigrationName "CustomerMigration" -MigrationRunId "20250603_143000"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $MigrationName,

        [Parameter(Mandatory = $true)]
        [string] $MigrationRunId
    )

    BEGIN {}

    PROCESS {
        $errorLog = @{
            migrationName  = $MigrationName
            migrationRunId = $MigrationRunId
            generatedAt    = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            totalEntries   = 0
            entries        = @()
        }

        return $errorLog
    }

    END {}
}


Function Add-ErrorLogEntry {
    <#
    .SYNOPSIS
        Adds an error entry to the error log.
     
    .NOTES
        Name: Add-ErrorLogEntry
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Add-ErrorLogEntry -ErrorLogData $errorLog -Message "Connection failed" -Table "dbo.Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $ErrorLogData,

        [Parameter(Mandatory = $true)]
        [string] $Message,

        [Parameter(Mandatory = $false)]
        [string] $Table
    )

    BEGIN {}

    PROCESS {
        $entry = @{
            timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            level     = "Error"
            table     = $Table
            message   = $Message
        }

        $ErrorLogData.entries += $entry
        $ErrorLogData.totalEntries++
    }

    END {}
}


Function Write-ErrorLogOutput {
    <#
    .SYNOPSIS
        Writes the error log JSON file.
     
    .NOTES
        Name: Write-ErrorLogOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Write-ErrorLogOutput -FilePath $errorLogFile -ErrorLogData $errorLog
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,

        [Parameter(Mandatory = $true)]
        [hashtable] $ErrorLogData
    )

    BEGIN {}

    PROCESS {
        $ErrorLogData.generatedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        $json = $ErrorLogData | ConvertTo-Json -Depth 10
        Set-Content -Path $FilePath -Value $json -Encoding UTF8

        Write-Log -Message "Error log file updated: $FilePath (Total entries: $($ErrorLogData.totalEntries))" -Level Debug
    }

    END {}
}


Function Write-ValidationOutput {
    <#
    .SYNOPSIS
        Writes the validation output JSON file.
     
    .NOTES
        Name: Write-ValidationOutput
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Write-ValidationOutput -FilePath $validationFile -ValidationData $validation
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,

        [Parameter(Mandatory = $true)]
        [hashtable] $ValidationData
    )

    BEGIN {}

    PROCESS {
        $json = $ValidationData | ConvertTo-Json -Depth 10
        Set-Content -Path $FilePath -Value $json -Encoding UTF8

        Write-Log -Message "Validation output written: $FilePath" -Level Info
    }

    END {}
}