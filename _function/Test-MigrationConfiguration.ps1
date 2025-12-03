Function Test-MigrationConfiguration {
    <#
    .SYNOPSIS
        Validates the entire migration configuration without executing the migration.
     
    .NOTES
        Name: Test-MigrationConfiguration
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $validation = Test-MigrationConfiguration -MasterConfig $masterConfig -RootPath "C:\Migration" -TableMappings $tableMappings
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $MasterConfig,

        [Parameter(Mandatory = $true)]
        [string] $RootPath,

        [Parameter(Mandatory = $false)]
        [hashtable[]] $TableMappings = @()
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Starting migration configuration validation..." -Level Info

        $validation = @{
            migrationName = $MasterConfig.MigrationName
            validatedAt   = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            isValid       = $true
            summary       = @{
                tablesValidated = 0
                errorsFound     = 0
                warningsFound   = 0
            }
            configuration = @{
                isValid  = $true
                errors   = @()
                warnings = @()
            }
            connections   = @{
                source = @{
                    isValid  = $false
                    provider = $MasterConfig.SourceConnection.Provider
                    server   = $MasterConfig.SourceConnection.Server
                    message  = ""
                }
                target = @{
                    isValid  = $false
                    provider = $MasterConfig.TargetConnection.Provider
                    server   = $MasterConfig.TargetConnection.Server
                    message  = ""
                }
            }
            tables        = @()
        }

        # Validate configuration structure
        $configResult = Test-ConfigurationStructure -MasterConfig $MasterConfig -RootPath $RootPath
        $validation.configuration = $configResult
        if (-not $configResult.isValid) {
            $validation.isValid = $false
            $validation.summary.errorsFound += $configResult.errors.Count
        }
        $validation.summary.warningsFound += $configResult.warnings.Count

        # Validate connections
        $sourceConnResult = Test-DatabaseConnection -ConnectionConfig $MasterConfig.SourceConnection -ConnectionName "Source" -TimeoutSeconds $MasterConfig.QueryTimeoutSeconds
        $validation.connections.source = $sourceConnResult
        if (-not $sourceConnResult.isValid) {
            $validation.isValid = $false
            $validation.summary.errorsFound++
        }

        $targetConnResult = Test-DatabaseConnection -ConnectionConfig $MasterConfig.TargetConnection -ConnectionName "Target" -TimeoutSeconds $MasterConfig.QueryTimeoutSeconds
        $validation.connections.target = $targetConnResult
        if (-not $targetConnResult.isValid) {
            $validation.isValid = $false
            $validation.summary.errorsFound++
        }

        # Only validate tables if both connections succeeded
        if ($sourceConnResult.isValid -and $targetConnResult.isValid) {
            $sourceConnection = $sourceConnResult.connection
            $targetConnection = $targetConnResult.connection

            try {
                foreach ($tableConfig in $MasterConfig.Tables) {
                    $tableMapping = $null
                    if ($tableConfig.File) {
                        $tableMapping = $TableMappings | Where-Object { 
                            $_.SourceSchema -eq $tableConfig.SourceSchema -and $_.SourceTable -eq $tableConfig.SourceTable 
                        } | Select-Object -First 1
                    }

                    $tableResult = Test-TableConfiguration -SourceConnection $sourceConnection -TargetConnection $targetConnection -TableConfig $tableConfig -TableMapping $tableMapping -SourceProvider $MasterConfig.SourceConnection.Provider

                    $validation.tables += $tableResult
                    $validation.summary.tablesValidated++

                    if (-not $tableResult.isValid) {
                        $validation.isValid = $false
                        $validation.summary.errorsFound += $tableResult.errors.Count
                    }
                    $validation.summary.warningsFound += $tableResult.warnings.Count
                }
            }
            finally {
                # Close connections
                if ($sourceConnection) { Disconnect-Database -Connection $sourceConnection -ConnectionName "Source" }
                if ($targetConnection) { Disconnect-Database -Connection $targetConnection -ConnectionName "Target" }
            }
        }

        Write-Log -Message "Validation complete. Valid: $($validation.isValid), Errors: $($validation.summary.errorsFound), Warnings: $($validation.summary.warningsFound)" -Level Info

        return $validation
    }

    END {}
}


Function Test-ConfigurationStructure {
    <#
    .SYNOPSIS
        Validates the structure and completeness of configuration files.
     
    .NOTES
        Name: Test-ConfigurationStructure
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $MasterConfig,

        [Parameter(Mandatory = $true)]
        [string] $RootPath
    )

    BEGIN {}

    PROCESS {
        $result = @{
            isValid  = $true
            errors   = @()
            warnings = @()
        }

        Write-Log -Message "Validating configuration structure..." -Level Verbose

        # Check required master config fields
        if ([string]::IsNullOrWhiteSpace($MasterConfig.MigrationName)) {
            $result.errors += "MigrationName is required"
            $result.isValid = $false
        }

        if ($null -eq $MasterConfig.BatchSize) {
            $result.warnings += "BatchSize not specified, defaulting to 0 (no batching)"
        }

        if ($null -eq $MasterConfig.QueryTimeoutSeconds -or $MasterConfig.QueryTimeoutSeconds -le 0) {
            $result.warnings += "QueryTimeoutSeconds not specified or invalid, defaulting to 300"
        }

        # Check source connection
        if ([string]::IsNullOrWhiteSpace($MasterConfig.SourceConnection.Provider)) {
            $result.errors += "Source connection Provider is required"
            $result.isValid = $false
        } elseif ($MasterConfig.SourceConnection.Provider -notin @("SqlServer", "AzureSql", "Oracle", "MySql", "PostgreSql")) {
            $result.errors += "Invalid source Provider: $($MasterConfig.SourceConnection.Provider)"
            $result.isValid = $false
        }

        if ([string]::IsNullOrWhiteSpace($MasterConfig.SourceConnection.Server)) {
            $result.errors += "Source connection Server is required"
            $result.isValid = $false
        }

        if ([string]::IsNullOrWhiteSpace($MasterConfig.SourceConnection.Database)) {
            $result.errors += "Source connection Database is required"
            $result.isValid = $false
        }

        # Check target connection
        if ([string]::IsNullOrWhiteSpace($MasterConfig.TargetConnection.Provider)) {
            $result.errors += "Target connection Provider is required"
            $result.isValid = $false
        } elseif ($MasterConfig.TargetConnection.Provider -notin @("SqlServer", "AzureSql")) {
            $result.errors += "Target Provider must be SqlServer or AzureSql: $($MasterConfig.TargetConnection.Provider)"
            $result.isValid = $false
        }

        if ([string]::IsNullOrWhiteSpace($MasterConfig.TargetConnection.Server)) {
            $result.errors += "Target connection Server is required"
            $result.isValid = $false
        }

        if ([string]::IsNullOrWhiteSpace($MasterConfig.TargetConnection.Database)) {
            $result.errors += "Target connection Database is required"
            $result.isValid = $false
        }

        # Check tables
        if ($null -eq $MasterConfig.Tables -or $MasterConfig.Tables.Count -eq 0) {
            $result.errors += "At least one table must be defined"
            $result.isValid = $false
        } else {
            $orders = @()
            foreach ($table in $MasterConfig.Tables) {
                # Check required fields
                if ([string]::IsNullOrWhiteSpace($table.SourceSchema)) {
                    $result.errors += "Source schema is required for table"
                    $result.isValid = $false
                }

                if ([string]::IsNullOrWhiteSpace($table.SourceTable)) {
                    $result.errors += "Source table is required"
                    $result.isValid = $false
                }

                if ([string]::IsNullOrWhiteSpace($table.TargetSchema)) {
                    $result.errors += "Target schema is required for table $($table.SourceTable)"
                    $result.isValid = $false
                }

                if ([string]::IsNullOrWhiteSpace($table.TargetTable)) {
                    $result.errors += "Target table is required for table $($table.SourceTable)"
                    $result.isValid = $false
                }

                if ([string]::IsNullOrWhiteSpace($table.BatchColumn)) {
                    $result.errors += "BatchColumn is required for table $($table.SourceTable)"
                    $result.isValid = $false
                }

                if ($null -eq $table.Order) {
                    $result.errors += "Order is required for table $($table.SourceTable)"
                    $result.isValid = $false
                } else {
                    if ($orders -contains $table.Order) {
                        $result.warnings += "Duplicate order value $($table.Order) found"
                    }
                    $orders += $table.Order
                }

                # Check column mappings
                if ($null -eq $table.ColumnMappings -or $table.ColumnMappings.Count -eq 0) {
                    $result.errors += "At least one column mapping is required for table $($table.SourceTable)"
                    $result.isValid = $false
                }

                # Check if table mapping file exists when referenced
                if (-not [string]::IsNullOrWhiteSpace($table.File)) {
                    $tableMappingPath = Join-Path -Path $RootPath -ChildPath "_migration\_tablemappings\$($table.File)"
                    if (-not (Test-Path $tableMappingPath)) {
                        $result.errors += "Table mapping file not found: $($table.File)"
                        $result.isValid = $false
                    }
                }
            }
        }

        return $result
    }

    END {}
}


Function Test-DatabaseConnection {
    <#
    .SYNOPSIS
        Tests a database connection and returns the open connection if successful.
     
    .NOTES
        Name: Test-DatabaseConnection
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $ConnectionConfig,

        [Parameter(Mandatory = $true)]
        [string] $ConnectionName,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $result = @{
            isValid    = $false
            provider   = $ConnectionConfig.Provider
            server     = $ConnectionConfig.Server
            message    = ""
            connection = $null
        }

        Write-Log -Message "Testing $ConnectionName connection to $($ConnectionConfig.Server)..." -Level Verbose

        try {
            $connection = Connect-Database -ConnectionConfig $ConnectionConfig -ConnectionName $ConnectionName -TimeoutSeconds $TimeoutSeconds

            $result.isValid = $true
            $result.message = "Connection successful"
            $result.connection = $connection

            Write-Log -Message "$ConnectionName connection test passed" -Level Info
        }
        catch {
            $result.isValid = $false
            $result.message = "Connection failed: $($_.Exception.Message)"

            Write-Log -Message "$ConnectionName connection test failed: $($_.Exception.Message)" -Level Error
        }

        return $result
    }

    END {}
}


Function Test-TableConfiguration {
    <#
    .SYNOPSIS
        Validates a single table configuration including schema, columns, and mappings.
     
    .NOTES
        Name: Test-TableConfiguration
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
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
        [string] $SourceProvider
    )

    BEGIN {}

    PROCESS {
        $sourceTableFull = "$($TableConfig.SourceSchema).$($TableConfig.SourceTable)"
        $targetTableFull = "$($TableConfig.TargetSchema).$($TableConfig.TargetTable)"

        Write-Log -Message "Validating table: $sourceTableFull -> $targetTableFull" -Level Verbose

        $result = @{
            sourceTable    = $sourceTableFull
            targetTable    = $targetTableFull
            isValid        = $true
            sourceRowCount = 0
            errors         = @()
            warnings       = @()
            sampleData     = @()
        }

        # Validate source table exists and get columns
        $sourceColumns = Test-SourceTableExists -Connection $SourceConnection -Schema $TableConfig.SourceSchema -Table $TableConfig.SourceTable -Provider $SourceProvider

        if ($null -eq $sourceColumns) {
            $result.errors += "Source table does not exist: $sourceTableFull"
            $result.isValid = $false
            return $result
        }

        # Get source row count
        try {
            $result.sourceRowCount = Get-SourceRowCount -Connection $SourceConnection -Schema $TableConfig.SourceSchema -Table $TableConfig.SourceTable -Provider $SourceProvider -TimeoutSeconds 60
        }
        catch {
            $result.warnings += "Could not get source row count: $($_.Exception.Message)"
        }

        # Validate target table exists and get columns
        $targetColumns = Test-TargetTableExists -Connection $TargetConnection -Schema $TableConfig.TargetSchema -Table $TableConfig.TargetTable

        if ($null -eq $targetColumns) {
            $result.errors += "Target table does not exist: $targetTableFull"
            $result.isValid = $false
            return $result
        }

        # Validate batch column exists in source
        if ($TableConfig.BatchColumn -notin $sourceColumns) {
            $result.errors += "Batch column '$($TableConfig.BatchColumn)' does not exist in source table"
            $result.isValid = $false
        }

        # Validate column mappings
        $mappedTargetColumns = @()

        foreach ($mapping in $TableConfig.ColumnMappings) {
            if ($mapping.Source -notin $sourceColumns) {
                $result.errors += "Source column '$($mapping.Source)' does not exist in source table"
                $result.isValid = $false
            }

            if ($mapping.Target -notin $targetColumns) {
                $result.errors += "Target column '$($mapping.Target)' does not exist in target table"
                $result.isValid = $false
            }

            $mappedTargetColumns += $mapping.Target
        }

        # Validate table mapping if exists
        if ($TableMapping) {
            # Validate source/target match
            if ($TableMapping.SourceSchema -ne $TableConfig.SourceSchema -or $TableMapping.SourceTable -ne $TableConfig.SourceTable) {
                $result.errors += "Table mapping source does not match MasterConfig"
                $result.isValid = $false
            }

            if ($TableMapping.TargetSchema -ne $TableConfig.TargetSchema -or $TableMapping.TargetTable -ne $TableConfig.TargetTable) {
                $result.errors += "Table mapping target does not match MasterConfig"
                $result.isValid = $false
            }

            # Validate transformations
            foreach ($transform in $TableMapping.Columns) {
                $transformResult = Test-TransformConfiguration -Transformation $transform -SourceColumns $sourceColumns -TargetColumns $targetColumns -TableConfig $TableConfig -AllTables $null

                if ($transformResult.errors.Count -gt 0) {
                    $result.errors += $transformResult.errors
                    $result.isValid = $false
                }
                $result.warnings += $transformResult.warnings

                if ($transform.Target) {
                    $mappedTargetColumns += $transform.Target
                }
                if ($transform.Targets) {
                    foreach ($t in $transform.Targets) {
                        $mappedTargetColumns += $t.Column
                    }
                }
            }

            # Validate settings
            if ($TableMapping.Settings.IdentityHandling -notin @("PreserveKeys", "GenerateNew", $null, "")) {
                $result.errors += "Invalid IdentityHandling value: $($TableMapping.Settings.IdentityHandling)"
                $result.isValid = $false
            }

            if ($TableMapping.Settings.ExistingDataAction -notin @("Truncate", "Append", $null, "")) {
                $result.errors += "Invalid ExistingDataAction value: $($TableMapping.Settings.ExistingDataAction)"
                $result.isValid = $false
            }
        }

        # Check for unmapped target columns
        $unmappedTargetColumns = $targetColumns | Where-Object { $_ -notin $mappedTargetColumns }
        foreach ($col in $unmappedTargetColumns) {
            $result.warnings += "Target column '$col' is not mapped and will use default value"
        }

        # Check for unmapped source columns
        $mappedSourceColumns = $TableConfig.ColumnMappings | ForEach-Object { $_.Source }
        $unmappedSourceColumns = $sourceColumns | Where-Object { $_ -notin $mappedSourceColumns }
        foreach ($col in $unmappedSourceColumns) {
            $result.warnings += "Source column '$col' is not mapped and will be ignored"
        }

        # Get sample data
        try {
            $result.sampleData = Get-SampleTransformedData -SourceConnection $SourceConnection -TableConfig $TableConfig -TableMapping $TableMapping -SourceProvider $SourceProvider -SampleSize 3
        }
        catch {
            $result.warnings += "Could not generate sample data: $($_.Exception.Message)"
        }

        return $result
    }

    END {}
}


Function Test-SourceTableExists {
    <#
    .SYNOPSIS
        Checks if a source table exists and returns its column names.
     
    .NOTES
        Name: Test-SourceTableExists
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
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
        [string] $Provider
    )

    BEGIN {}

    PROCESS {
        $sql = switch ($Provider) {
            "Oracle" {
                "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = UPPER('$Schema') AND TABLE_NAME = UPPER('$Table') ORDER BY COLUMN_ID"
            }
            "MySql" {
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '$Schema' AND TABLE_NAME = '$Table' ORDER BY ORDINAL_POSITION"
            }
            "PostgreSql" {
                "SELECT column_name FROM information_schema.columns WHERE table_schema = '$Schema' AND table_name = '$Table' ORDER BY ordinal_position"
            }
            default {
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '$Schema' AND TABLE_NAME = '$Table' ORDER BY ORDINAL_POSITION"
            }
        }

        $columns = @()

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = 30

            $reader = $command.ExecuteReader()

            while ($reader.Read()) {
                $columns += $reader[0].ToString()
            }

            $reader.Close()

            if ($columns.Count -eq 0) {
                return $null
            }

            return $columns
        }
        catch {
            Write-Log -Message "Failed to check source table [$Schema].[$Table]: $($_.Exception.Message)" -Level Debug
            return $null
        }
        finally {
            if ($reader) { $reader.Dispose() }
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Test-TargetTableExists {
    <#
    .SYNOPSIS
        Checks if a target table exists and returns its column names.
     
    .NOTES
        Name: Test-TargetTableExists
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $Schema,

        [Parameter(Mandatory = $true)]
        [string] $Table
    )

    BEGIN {}

    PROCESS {
        $columns = Get-TargetTableColumns -Connection $Connection -Schema $Schema -Table $Table -TimeoutSeconds 30

        if ($columns.Count -eq 0) {
            return $null
        }

        return $columns
    }

    END {}
}


Function Test-TransformConfiguration {
    <#
    .SYNOPSIS
        Validates a transformation configuration.
     
    .NOTES
        Name: Test-TransformConfiguration
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [array] $SourceColumns,

        [Parameter(Mandatory = $true)]
        [array] $TargetColumns,

        [Parameter(Mandatory = $true)]
        [hashtable] $TableConfig,

        [Parameter(Mandatory = $false)]
        [array] $AllTables
    )

    BEGIN {}

    PROCESS {
        $result = @{
            errors   = @()
            warnings = @()
        }

        $type = $Transformation.Type

        switch ($type) {
            "simple" {
                if ($Transformation.Source -notin $SourceColumns) {
                    $result.errors += "Transform source column '$($Transformation.Source)' does not exist"
                }
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Transform target column '$($Transformation.Target)' does not exist"
                }
            }

            "concat" {
                foreach ($part in $Transformation.Parts) {
                    if ($part.Type -eq "column" -and $part.Value -notin $SourceColumns) {
                        $result.errors += "Concat source column '$($part.Value)' does not exist"
                    }
                }
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Concat target column '$($Transformation.Target)' does not exist"
                }
            }

            "split" {
                if ($Transformation.Source -notin $SourceColumns) {
                    $result.errors += "Split source column '$($Transformation.Source)' does not exist"
                }
                foreach ($target in $Transformation.Targets) {
                    if ($target.Column -notin $TargetColumns) {
                        $result.errors += "Split target column '$($target.Column)' does not exist"
                    }
                }
            }

            "lookup" {
                if ($Transformation.Source -notin $SourceColumns) {
                    $result.errors += "Lookup source column '$($Transformation.Source)' does not exist"
                }
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Lookup target column '$($Transformation.Target)' does not exist"
                }
                if ($null -eq $Transformation.LookupTable -or $Transformation.LookupTable.Count -eq 0) {
                    $result.warnings += "Lookup table is empty for column '$($Transformation.Source)'"
                }
            }

            "calculated" {
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Calculated target column '$($Transformation.Target)' does not exist"
                }
                if ([string]::IsNullOrWhiteSpace($Transformation.Expression)) {
                    $result.errors += "Calculated expression is empty"
                }
            }

            "static" {
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Static target column '$($Transformation.Target)' does not exist"
                }
            }

            "conditional" {
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Conditional target column '$($Transformation.Target)' does not exist"
                }
                if ($null -eq $Transformation.Conditions -or $Transformation.Conditions.Count -eq 0) {
                    $result.warnings += "No conditions defined for conditional transform"
                }
            }

            "convert" {
                if ($Transformation.Source -notin $SourceColumns) {
                    $result.errors += "Convert source column '$($Transformation.Source)' does not exist"
                }
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "Convert target column '$($Transformation.Target)' does not exist"
                }
            }

            "keyLookup" {
                if ($Transformation.Source -notin $SourceColumns) {
                    $result.errors += "KeyLookup source column '$($Transformation.Source)' does not exist"
                }
                if ($Transformation.Target -notin $TargetColumns) {
                    $result.errors += "KeyLookup target column '$($Transformation.Target)' does not exist"
                }
                if ([string]::IsNullOrWhiteSpace($Transformation.KeyMapSourceTable)) {
                    $result.errors += "KeyLookup sourceTable is required"
                }
                if ([string]::IsNullOrWhiteSpace($Transformation.KeyMapSourceKeyColumn)) {
                    $result.errors += "KeyLookup sourceKeyColumn is required"
                }
            }

            default {
                $result.errors += "Unknown transformation type: $type"
            }
        }

        return $result
    }

    END {}
}


Function Get-SampleTransformedData {
    <#
    .SYNOPSIS
        Gets sample rows from source and shows before/after transformation.
     
    .NOTES
        Name: Get-SampleTransformedData
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $SourceConnection,

        [Parameter(Mandatory = $true)]
        [hashtable] $TableConfig,

        [Parameter(Mandatory = $false)]
        [hashtable] $TableMapping,

        [Parameter(Mandatory = $true)]
        [string] $SourceProvider,

        [Parameter(Mandatory = $false)]
        [int] $SampleSize = 3
    )

    BEGIN {}

    PROCESS {
        $sampleData = @()

        # Get sample source rows
        $sourceRows = Get-SourceDataBatch -Connection $SourceConnection -Schema $TableConfig.SourceSchema -Table $TableConfig.SourceTable -BatchColumn $TableConfig.BatchColumn -BatchSize $SampleSize -Provider $SourceProvider -TimeoutSeconds 30

        $transformations = if ($TableMapping) { $TableMapping.Columns } else { @() }

        foreach ($sourceRow in $sourceRows) {
            $transformedRow = Convert-SourceRowToTarget -SourceRow $sourceRow -ColumnMappings $TableConfig.ColumnMappings -Transformations $transformations -KeyMaps @{}

            # Remove internal tracking key
            $transformedRow.Remove("__SourceKey")

            $sample = @{
                source      = $sourceRow
                transformed = $transformedRow
            }

            $sampleData += $sample
        }

        return $sampleData
    }

    END {}
}