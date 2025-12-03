Function Get-SourceRowCount {
    <#
    .SYNOPSIS
        Gets the total row count from the source table.
     
    .NOTES
        Name: Get-SourceRowCount
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $count = Get-SourceRowCount -Connection $conn -Schema "dbo" -Table "Customers" -Provider "SqlServer"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $Schema,

        [Parameter(Mandatory = $true)]
        [string] $Table,

        [Parameter(Mandatory = $true)]
        [string] $Provider,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        $sql = switch ($Provider) {
            "Oracle" { "SELECT COUNT(*) FROM $Schema.$Table" }
            default { "SELECT COUNT(*) FROM [$Schema].[$Table]" }
        }

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $count = [int]$command.ExecuteScalar()

            Write-Log -Message "Source row count for [$Schema].[$Table]: $count" -Level Info -Table "$Schema.$Table"

            return $count
        }
        catch {
            Write-Log -Message "Failed to get row count from [$Schema].[$Table]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Get-SourceDataBatch {
    <#
    .SYNOPSIS
        Retrieves a batch of rows from the source table.
     
    .NOTES
        Name: Get-SourceDataBatch
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $rows = Get-SourceDataBatch -Connection $conn -Schema "dbo" -Table "Customers" -BatchColumn "CustID" -BatchSize 10000 -LastKeyValue "0" -Provider "SqlServer"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $Schema,

        [Parameter(Mandatory = $true)]
        [string] $Table,

        [Parameter(Mandatory = $true)]
        [string] $BatchColumn,

        [Parameter(Mandatory = $true)]
        [int] $BatchSize,

        [Parameter(Mandatory = $false)]
        [string] $LastKeyValue,

        [Parameter(Mandatory = $true)]
        [string] $Provider,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        # Build SQL based on provider
        $sql = switch ($Provider) {
            "Oracle" {
                if ($LastKeyValue) {
                    "SELECT * FROM $Schema.$Table WHERE $BatchColumn > :lastKey ORDER BY $BatchColumn FETCH FIRST $BatchSize ROWS ONLY"
                } else {
                    "SELECT * FROM $Schema.$Table ORDER BY $BatchColumn FETCH FIRST $BatchSize ROWS ONLY"
                }
            }
            "MySql" {
                if ($LastKeyValue) {
                    "SELECT * FROM ``$Schema``.``$Table`` WHERE ``$BatchColumn`` > @lastKey ORDER BY ``$BatchColumn`` LIMIT $BatchSize"
                } else {
                    "SELECT * FROM ``$Schema``.``$Table`` ORDER BY ``$BatchColumn`` LIMIT $BatchSize"
                }
            }
            "PostgreSql" {
                if ($LastKeyValue) {
                    "SELECT * FROM `"$Schema`".`"$Table`" WHERE `"$BatchColumn`" > @lastKey ORDER BY `"$BatchColumn`" LIMIT $BatchSize"
                } else {
                    "SELECT * FROM `"$Schema`".`"$Table`" ORDER BY `"$BatchColumn`" LIMIT $BatchSize"
                }
            }
            default {
                # SqlServer, AzureSql
                if ($LastKeyValue) {
                    "SELECT TOP $BatchSize * FROM [$Schema].[$Table] WHERE [$BatchColumn] > @lastKey ORDER BY [$BatchColumn]"
                } else {
                    "SELECT TOP $BatchSize * FROM [$Schema].[$Table] ORDER BY [$BatchColumn]"
                }
            }
        }

        $rows = @()

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            if ($LastKeyValue) {
                $param = $command.CreateParameter()
                $param.ParameterName = if ($Provider -eq "Oracle") { ":lastKey" } else { "@lastKey" }
                $param.Value = $LastKeyValue
                $command.Parameters.Add($param) | Out-Null
            }

            $reader = $command.ExecuteReader()

            while ($reader.Read()) {
                $row = @{}

                for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                    $columnName = $reader.GetName($i)
                    $value = $reader.GetValue($i)
                    $row[$columnName] = $value
                }

                $rows += $row
            }

            $reader.Close()

            Write-Log -Message "Retrieved $($rows.Count) rows from [$Schema].[$Table]" -Level Debug -Table "$Schema.$Table"

            return $rows
        }
        catch {
            Write-Log -Message "Failed to retrieve batch from [$Schema].[$Table]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($reader) { $reader.Dispose() }
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Get-AllSourceData {
    <#
    .SYNOPSIS
        Retrieves all rows from the source table (no batching).
     
    .NOTES
        Name: Get-AllSourceData
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $rows = Get-AllSourceData -Connection $conn -Schema "dbo" -Table "Customers" -Provider "SqlServer"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $Schema,

        [Parameter(Mandatory = $true)]
        [string] $Table,

        [Parameter(Mandatory = $true)]
        [string] $Provider,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 600
    )

    BEGIN {}

    PROCESS {
        $sql = switch ($Provider) {
            "Oracle" { "SELECT * FROM $Schema.$Table" }
            "MySql" { "SELECT * FROM ``$Schema``.``$Table``" }
            "PostgreSql" { "SELECT * FROM `"$Schema`".`"$Table`"" }
            default { "SELECT * FROM [$Schema].[$Table]" }
        }

        $rows = @()

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $reader = $command.ExecuteReader()

            while ($reader.Read()) {
                $row = @{}

                for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                    $columnName = $reader.GetName($i)
                    $value = $reader.GetValue($i)
                    $row[$columnName] = $value
                }

                $rows += $row
            }

            $reader.Close()

            Write-Log -Message "Retrieved all $($rows.Count) rows from [$Schema].[$Table]" -Level Debug -Table "$Schema.$Table"

            return $rows
        }
        catch {
            Write-Log -Message "Failed to retrieve data from [$Schema].[$Table]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($reader) { $reader.Dispose() }
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Convert-SourceRowToTarget {
    <#
    .SYNOPSIS
        Transforms a source row to a target row by applying all mappings and transformations.
     
    .NOTES
        Name: Convert-SourceRowToTarget
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $targetRow = Convert-SourceRowToTarget -SourceRow $row -ColumnMappings $mappings -Transformations $transforms -KeyMaps $keyMaps
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow,

        [Parameter(Mandatory = $true)]
        [array] $ColumnMappings,

        [Parameter(Mandatory = $false)]
        [array] $Transformations = @(),

        [Parameter(Mandatory = $false)]
        [hashtable] $KeyMaps = @{}
    )

    BEGIN {}

    PROCESS {
        $targetRow = @{}

        # Get list of columns handled by transformations
        $transformedTargetColumns = @()
        foreach ($transform in $Transformations) {
            if ($transform.Target) {
                $transformedTargetColumns += $transform.Target
            }
            if ($transform.Targets) {
                foreach ($t in $transform.Targets) {
                    $transformedTargetColumns += $t.Column
                }
            }
        }

        # Apply simple column mappings (skip if transformation exists for this target)
        foreach ($mapping in $ColumnMappings) {
            if ($mapping.Target -notin $transformedTargetColumns) {
                $result = Invoke-SimpleColumnMapping -Mapping $mapping -SourceRow $SourceRow
                foreach ($key in $result.Keys) {
                    $targetRow[$key] = $result[$key]
                }
            }
        }

        # Apply transformations
        foreach ($transform in $Transformations) {
            $result = Invoke-ColumnTransform -Transformation $transform -SourceRow $SourceRow -KeyMaps $KeyMaps

            foreach ($key in $result.Keys) {
                $targetRow[$key] = $result[$key]
            }
        }

        return $targetRow
    }

    END {}
}


Function Invoke-TableMigration {
    <#
    .SYNOPSIS
        Orchestrates the migration of a single table from source to target.
     
    .NOTES
        Name: Invoke-TableMigration
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Invoke-TableMigration -SourceConnection $srcConn -TargetConnection $tgtConn -TableConfig $tableConfig -TableMapping $tableMapping -MasterConfig $masterConfig -ProgressData $progress -RowErrorsData $rowErrors -ErrorLogData $errorLog -KeyMaps $keyMaps
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $SourceConnection,

        [Parameter(Mandatory = $true)]
        [object] $TargetConnection,

        [Parameter(Mandatory = $true)]
        [hashtable] $TableConfig,

        [Parameter(Mandatory = $false)]
        [hashtable] $TableMapping,

        [Parameter(Mandatory = $true)]
        [hashtable] $MasterConfig,

        [Parameter(Mandatory = $true)]
        [hashtable] $ProgressData,

        [Parameter(Mandatory = $true)]
        [hashtable] $RowErrorsData,

        [Parameter(Mandatory = $true)]
        [hashtable] $ErrorLogData,

        [Parameter(Mandatory = $true)]
        [string] $ProgressFilePath,

        [Parameter(Mandatory = $true)]
        [string] $RowErrorsFilePath,

        [Parameter(Mandatory = $true)]
        [string] $ErrorLogFilePath,

        [Parameter(Mandatory = $false)]
        [hashtable] $KeyMaps = @{}
    )

    BEGIN {}

    PROCESS {
        $sourceSchema = $TableConfig.SourceSchema
        $sourceTable = $TableConfig.SourceTable
        $targetSchema = $TableConfig.TargetSchema
        $targetTable = $TableConfig.TargetTable
        $batchColumn = $TableConfig.BatchColumn
        $columnMappings = $TableConfig.ColumnMappings
        $sourceTableFull = "$sourceSchema.$sourceTable"
        $targetTableFull = "$targetSchema.$targetTable"

        $batchSize = $MasterConfig.BatchSize
        $queryTimeout = $MasterConfig.QueryTimeoutSeconds
        $sourceProvider = $MasterConfig.SourceConnection.Provider

        # Get settings from table mapping if exists
        $identityHandling = if ($TableMapping) { $TableMapping.Settings.IdentityHandling } else { "PreserveKeys" }
        $identityColumn = if ($TableMapping) { $TableMapping.Settings.IdentityColumn } else { $null }
        $existingDataAction = if ($TableMapping) { $TableMapping.Settings.ExistingDataAction } else { "Append" }
        $transformations = if ($TableMapping) { $TableMapping.Columns } else { @() }

        Write-Log -Message "Starting migration: $sourceTableFull -> $targetTableFull" -Level Info -Table $sourceTableFull

        # Update progress
        Update-TableProgress -ProgressData $ProgressData -SourceTable $sourceTableFull -Status "InProgress"
        Write-ProgressOutput -FilePath $ProgressFilePath -ProgressData $ProgressData

        try {
            # Get source row count
            $totalRows = Get-SourceRowCount -Connection $SourceConnection -Schema $sourceSchema -Table $sourceTable -Provider $sourceProvider -TimeoutSeconds $queryTimeout
            Update-TableProgress -ProgressData $ProgressData -SourceTable $sourceTableFull -TotalRows $totalRows
            Write-ProgressOutput -FilePath $ProgressFilePath -ProgressData $ProgressData

            # Handle existing data action
            if ($existingDataAction -eq "Truncate") {
                Truncate-TargetTable -Connection $TargetConnection -Schema $targetSchema -Table $targetTable -TimeoutSeconds $queryTimeout
            }

            # Disable triggers on target table
            Disable-TableTriggers -Connection $TargetConnection -Schema $targetSchema -Table $targetTable

            # Check if we need to create key map table
            $createKeyMap = ($identityHandling -eq "GenerateNew" -and $identityColumn)
            if ($createKeyMap) {
                New-KeyMapTable -Connection $TargetConnection -SourceTable $sourceTableFull
            }

            # Get target identity column if preserving keys
            $targetIdentityColumn = Get-TargetIdentityColumn -Connection $TargetConnection -Schema $targetSchema -Table $targetTable

            # Get target columns for DataTable
            $targetColumns = Get-TargetTableColumns -Connection $TargetConnection -Schema $targetSchema -Table $targetTable

            # Determine columns to insert (exclude identity if generating new)
            $insertColumns = if ($identityHandling -eq "GenerateNew" -and $identityColumn) {
                $targetColumns | Where-Object { $_ -ne $identityColumn }
            } else {
                $targetColumns
            }

            $processedRows = 0
            $lastKeyValue = $null

            # Check for resume
            $tableProgress = $ProgressData.tables | Where-Object { $_.sourceTable -eq $sourceTableFull }
            if ($tableProgress -and $tableProgress.lastBatchKeyValue) {
                $lastKeyValue = $tableProgress.lastBatchKeyValue
                $processedRows = $tableProgress.processedRows
                Write-Log -Message "Resuming from key value: $lastKeyValue (processed: $processedRows)" -Level Info -Table $sourceTableFull
            }

            # Process in batches or all at once
            if ($batchSize -eq 0) {
                # No batching - get all rows
                $sourceRows = Get-AllSourceData -Connection $SourceConnection -Schema $sourceSchema -Table $sourceTable -Provider $sourceProvider -TimeoutSeconds $queryTimeout

                $result = Invoke-BatchProcess -SourceRows $sourceRows -TargetConnection $TargetConnection -TargetSchema $targetSchema -TargetTable $targetTable -InsertColumns $insertColumns -ColumnMappings $columnMappings -Transformations $transformations -KeyMaps $KeyMaps -BatchColumn $batchColumn -IdentityHandling $identityHandling -IdentityColumn $identityColumn -TargetIdentityColumn $targetIdentityColumn -SourceTableFull $sourceTableFull -RowErrorsData $RowErrorsData -ErrorLogData $ErrorLogData -RowErrorsFilePath $RowErrorsFilePath -ErrorLogFilePath $ErrorLogFilePath -QueryTimeout $queryTimeout

                $processedRows += $result.ProcessedCount
                $lastKeyValue = $result.LastKeyValue

                # Save key mappings if generated
                if ($result.KeyMappings -and $result.KeyMappings.Count -gt 0) {
                    Add-KeyMapEntryBatch -Connection $TargetConnection -SourceTable $sourceTableFull -KeyMappings $result.KeyMappings
                }

                Update-TableProgress -ProgressData $ProgressData -SourceTable $sourceTableFull -ProcessedRows $processedRows -LastBatchKeyValue $lastKeyValue
                Write-ProgressOutput -FilePath $ProgressFilePath -ProgressData $ProgressData
            } else {
                # Process in batches
                do {
                    $sourceRows = Get-SourceDataBatch -Connection $SourceConnection -Schema $sourceSchema -Table $sourceTable -BatchColumn $batchColumn -BatchSize $batchSize -LastKeyValue $lastKeyValue -Provider $sourceProvider -TimeoutSeconds $queryTimeout

                    if ($sourceRows.Count -eq 0) {
                        break
                    }

                    $result = Invoke-BatchProcess -SourceRows $sourceRows -TargetConnection $TargetConnection -TargetSchema $targetSchema -TargetTable $targetTable -InsertColumns $insertColumns -ColumnMappings $columnMappings -Transformations $transformations -KeyMaps $KeyMaps -BatchColumn $batchColumn -IdentityHandling $identityHandling -IdentityColumn $identityColumn -TargetIdentityColumn $targetIdentityColumn -SourceTableFull $sourceTableFull -RowErrorsData $RowErrorsData -ErrorLogData $ErrorLogData -RowErrorsFilePath $RowErrorsFilePath -ErrorLogFilePath $ErrorLogFilePath -QueryTimeout $queryTimeout

                    $processedRows += $result.ProcessedCount
                    $lastKeyValue = $result.LastKeyValue

                    # Save key mappings if generated
                    if ($result.KeyMappings -and $result.KeyMappings.Count -gt 0) {
                        Add-KeyMapEntryBatch -Connection $TargetConnection -SourceTable $sourceTableFull -KeyMappings $result.KeyMappings
                    }

                    Update-TableProgress -ProgressData $ProgressData -SourceTable $sourceTableFull -ProcessedRows $processedRows -LastBatchKeyValue $lastKeyValue
                    Write-ProgressOutput -FilePath $ProgressFilePath -ProgressData $ProgressData

                    Write-Log -Message "Batch complete: $processedRows / $totalRows rows processed" -Level Info -Table $sourceTableFull

                } while ($sourceRows.Count -eq $batchSize)
            }

            # Load key mappings into memory for child tables
            if ($createKeyMap) {
                $KeyMaps[$sourceTableFull] = Get-AllKeyMappings -Connection $TargetConnection -SourceTable $sourceTableFull
            }

            # Re-enable triggers
            Enable-TableTriggers -Connection $TargetConnection -Schema $targetSchema -Table $targetTable

            # Mark complete
            Update-TableProgress -ProgressData $ProgressData -SourceTable $sourceTableFull -Status "Completed" -ProcessedRows $processedRows
            Write-ProgressOutput -FilePath $ProgressFilePath -ProgressData $ProgressData

            Write-Log -Message "Migration complete: $sourceTableFull -> $targetTableFull ($processedRows rows)" -Level Info -Table $sourceTableFull
        }
        catch {
            Update-TableProgress -ProgressData $ProgressData -SourceTable $sourceTableFull -Status "Failed"
            Write-ProgressOutput -FilePath $ProgressFilePath -ProgressData $ProgressData

            Add-ErrorLogEntry -ErrorLogData $ErrorLogData -Message "Table migration failed: $($_.Exception.Message)" -Table $sourceTableFull
            Write-ErrorLogOutput -FilePath $ErrorLogFilePath -ErrorLogData $ErrorLogData

            Write-Log -Message "Migration failed for $sourceTableFull" -Level Error -Exception $_.Exception -Table $sourceTableFull
            throw
        }
    }

    END {}
}


Function Invoke-BatchProcess {
    <#
    .SYNOPSIS
        Processes a batch of source rows, transforms them, and inserts into target.
     
    .NOTES
        Name: Invoke-BatchProcess
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array] $SourceRows,

        [Parameter(Mandatory = $true)]
        [object] $TargetConnection,

        [Parameter(Mandatory = $true)]
        [string] $TargetSchema,

        [Parameter(Mandatory = $true)]
        [string] $TargetTable,

        [Parameter(Mandatory = $true)]
        [array] $InsertColumns,

        [Parameter(Mandatory = $true)]
        [array] $ColumnMappings,

        [Parameter(Mandatory = $false)]
        [array] $Transformations = @(),

        [Parameter(Mandatory = $false)]
        [hashtable] $KeyMaps = @{},

        [Parameter(Mandatory = $true)]
        [string] $BatchColumn,

        [Parameter(Mandatory = $true)]
        [string] $IdentityHandling,

        [Parameter(Mandatory = $false)]
        [string] $IdentityColumn,

        [Parameter(Mandatory = $false)]
        [string] $TargetIdentityColumn,

        [Parameter(Mandatory = $true)]
        [string] $SourceTableFull,

        [Parameter(Mandatory = $true)]
        [hashtable] $RowErrorsData,

        [Parameter(Mandatory = $true)]
        [hashtable] $ErrorLogData,

        [Parameter(Mandatory = $true)]
        [string] $RowErrorsFilePath,

        [Parameter(Mandatory = $true)]
        [string] $ErrorLogFilePath,

        [Parameter(Mandatory = $false)]
        [int] $QueryTimeout = 300
    )

    BEGIN {}

    PROCESS {
        $processedCount = 0
        $lastKeyValue = $null
        $keyMappings = @()
        $targetTableFull = "$TargetSchema.$TargetTable"

        # Transform all rows
        $transformedRows = @()

        foreach ($sourceRow in $SourceRows) {
            try {
                $targetRow = Convert-SourceRowToTarget -SourceRow $sourceRow -ColumnMappings $ColumnMappings -Transformations $Transformations -KeyMaps $KeyMaps

                # Store source key for key mapping
                $sourceKeyValue = $sourceRow[$BatchColumn]
                $targetRow["__SourceKey"] = $sourceKeyValue
                $lastKeyValue = $sourceKeyValue

                $transformedRows += $targetRow
            }
            catch {
                # Log row error and continue
                $sourceKeyValue = $sourceRow[$BatchColumn]
                $mappedSourceData = @{}
                foreach ($mapping in $ColumnMappings) {
                    if ($sourceRow.ContainsKey($mapping.Source)) {
                        $mappedSourceData[$mapping.Source] = $sourceRow[$mapping.Source]
                    }
                }

                Add-RowError -RowErrorsData $RowErrorsData -SourceTable $SourceTableFull -TargetTable $targetTableFull -SourceKeyValue $sourceKeyValue.ToString() -ErrorMessage "Transform error: $($_.Exception.Message)" -SourceData $mappedSourceData
                Write-RowErrorsOutput -FilePath $RowErrorsFilePath -RowErrorsData $RowErrorsData

                Write-Log -Message "Row transform failed for key $sourceKeyValue" -Level Warning -Table $SourceTableFull
            }
        }

        if ($transformedRows.Count -eq 0) {
            return @{
                ProcessedCount = 0
                LastKeyValue   = $lastKeyValue
                KeyMappings    = @()
            }
        }

        # Insert based on identity handling
        if ($IdentityHandling -eq "GenerateNew" -and $IdentityColumn) {
            # Insert row by row to capture new identities
            foreach ($row in $transformedRows) {
                $sourceKey = $row["__SourceKey"]
                $row.Remove("__SourceKey")

                # Filter to only insert columns
                $insertRow = @{}
                foreach ($col in $InsertColumns) {
                    if ($row.ContainsKey($col)) {
                        $insertRow[$col] = $row[$col]
                    }
                }

                try {
                    $newId = Invoke-SingleRowInsert -Connection $TargetConnection -Schema $TargetSchema -Table $TargetTable -RowData $insertRow -IdentityColumn $IdentityColumn -TimeoutSeconds $QueryTimeout

                    if ($newId) {
                        $keyMappings += @{
                            OldKey = $sourceKey.ToString()
                            NewKey = $newId.ToString()
                        }
                    }

                    $processedCount++
                }
                catch {
                    $mappedSourceData = @{}
                    foreach ($key in $insertRow.Keys) {
                        $mappedSourceData[$key] = $insertRow[$key]
                    }

                    Add-RowError -RowErrorsData $RowErrorsData -SourceTable $SourceTableFull -TargetTable $targetTableFull -SourceKeyValue $sourceKey.ToString() -ErrorMessage "Insert error: $($_.Exception.Message)" -SourceData $mappedSourceData
                    Write-RowErrorsOutput -FilePath $RowErrorsFilePath -RowErrorsData $RowErrorsData

                    Write-Log -Message "Row insert failed for key $sourceKey" -Level Warning -Table $SourceTableFull
                }
            }
        } else {
            # Bulk insert - preserve keys or no identity
            $dataTable = New-DataTable -ColumnNames $InsertColumns

            foreach ($row in $transformedRows) {
                $row.Remove("__SourceKey")

                # Filter to only insert columns
                $insertRow = @{}
                foreach ($col in $InsertColumns) {
                    if ($row.ContainsKey($col)) {
                        $insertRow[$col] = $row[$col]
                    }
                }

                Add-DataTableRow -DataTable $dataTable -RowData $insertRow
            }

            try {
                if ($IdentityHandling -eq "PreserveKeys" -and $TargetIdentityColumn) {
                    Invoke-BulkInsertWithIdentityPreserve -Connection $TargetConnection -Schema $TargetSchema -Table $TargetTable -DataTable $dataTable -IdentityColumn $TargetIdentityColumn -TimeoutSeconds $QueryTimeout
                } else {
                    Invoke-BatchInsert -Connection $TargetConnection -Schema $TargetSchema -Table $TargetTable -DataTable $dataTable -TimeoutSeconds $QueryTimeout
                }

                $processedCount = $dataTable.Rows.Count
            }
            catch {
                Add-ErrorLogEntry -ErrorLogData $ErrorLogData -Message "Batch insert failed: $($_.Exception.Message)" -Table $SourceTableFull
                Write-ErrorLogOutput -FilePath $ErrorLogFilePath -ErrorLogData $ErrorLogData

                Write-Log -Message "Batch insert failed" -Level Error -Exception $_.Exception -Table $SourceTableFull
                throw
            }
            finally {
                $dataTable.Dispose()
            }
        }

        return @{
            ProcessedCount = $processedCount
            LastKeyValue   = $lastKeyValue
            KeyMappings    = $keyMappings
        }
    }

    END {}
}