Function Read-GlobalConfig {
    <#
    .SYNOPSIS
        Reads the global script configuration XML file.
     
    .NOTES
        Name: Read-GlobalConfig
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $globalConfig = Read-GlobalConfig -Path ".\UberDooberDBMigrator.xml"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $Path
    )

    BEGIN {}

    PROCESS {
        if (-not (Test-Path $Path)) {
            throw "Global configuration file not found: $Path"
        }

        Write-Log -Message "Reading global configuration from $Path" -Level Verbose

        [xml]$xmlContent = Get-Content $Path
        
        $config = @{
            Environment     = $xmlContent.configuration.variables.Environment
            DefaultLogLevel = $xmlContent.configuration.variables.DefaultLogLevel
        }

        Write-Log -Message "Global config loaded - Environment: $($config.Environment)" -Level Debug

        return $config
    }

    END {}
}


Function Read-MasterConfig {
    <#
    .SYNOPSIS
        Reads the master migration configuration XML file.
     
    .NOTES
        Name: Read-MasterConfig
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $masterConfig = Read-MasterConfig -Path ".\_migration\MasterConfig.xml"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $Path
    )

    BEGIN {}

    PROCESS {
        if (-not (Test-Path $Path)) {
            throw "Master configuration file not found: $Path"
        }

        Write-Log -Message "Reading master configuration from $Path" -Level Verbose

        [xml]$xmlContent = Get-Content $Path
        $migrationNode = $xmlContent.MigrationConfig

        # Parse source connection
        $sourceConn = @{
            Provider = $migrationNode.SourceConnection.Provider
            Server   = $migrationNode.SourceConnection.Server
            Database = $migrationNode.SourceConnection.Database
            AuthType = $migrationNode.SourceConnection.AuthType
            UserId   = $migrationNode.SourceConnection.UserId
            Password = $migrationNode.SourceConnection.Password
        }

        # Parse target connection
        $targetConn = @{
            Provider = $migrationNode.TargetConnection.Provider
            Server   = $migrationNode.TargetConnection.Server
            Database = $migrationNode.TargetConnection.Database
            AuthType = $migrationNode.TargetConnection.AuthType
            UserId   = $migrationNode.TargetConnection.UserId
            Password = $migrationNode.TargetConnection.Password
        }

        # Parse tables
        $tables = @()
        foreach ($tableNode in $migrationNode.Tables.Table) {
            $table = @{
                Order          = [int]$tableNode.order
                TableFilter    = $tableNode.tableFilter -eq "true"
                File           = $tableNode.File
                SourceSchema   = $tableNode.Source.schema
                SourceTable    = $tableNode.Source.table
                TargetSchema   = $tableNode.Target.schema
                TargetTable    = $tableNode.Target.table
                BatchColumn    = $tableNode.BatchColumn
                ColumnMappings = @()
            }

            # Parse column mappings
            foreach ($colNode in $tableNode.ColumnMappings.Column) {
                $mapping = @{
                    Source       = $colNode.source
                    Target       = $colNode.target
                    SourceFormat = $colNode.sourceFormat
                }
                $table.ColumnMappings += $mapping
            }

            $tables += $table
        }

        # Sort tables by order
        $tables = $tables | Sort-Object -Property Order

        $config = @{
            MigrationName       = $migrationNode.MigrationName
            BatchSize           = [int]$migrationNode.BatchSize
            QueryTimeoutSeconds = [int]$migrationNode.QueryTimeoutSeconds
            SourceConnection    = $sourceConn
            TargetConnection    = $targetConn
            Tables              = $tables
        }

        Write-Log -Message "Master config loaded - Migration: $($config.MigrationName), Tables: $($tables.Count)" -Level Debug

        return $config
    }

    END {}
}


Function Read-TableMapping {
    <#
    .SYNOPSIS
        Reads a table mapping XML file for transformation definitions.
     
    .NOTES
        Name: Read-TableMapping
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $tableMapping = Read-TableMapping -Path ".\_migration\_tablemappings\SourceDB.Customers.xml"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $Path
    )

    BEGIN {}

    PROCESS {
        if (-not (Test-Path $Path)) {
            throw "Table mapping file not found: $Path"
        }

        Write-Log -Message "Reading table mapping from $Path" -Level Verbose

        [xml]$xmlContent = Get-Content $Path
        $mappingNode = $xmlContent.TableMapping

        # Parse source/target identification
        $mapping = @{
            SourceSchema   = $mappingNode.Source.schema
            SourceTable    = $mappingNode.Source.table
            SourceDatabase = $mappingNode.Source.database
            TargetSchema   = $mappingNode.Target.schema
            TargetTable    = $mappingNode.Target.table
            Settings       = @{}
            Columns        = @()
        }

        # Parse settings
        $settingsNode = $mappingNode.Settings
        if ($settingsNode) {
            $mapping.Settings = @{
                IdentityHandling   = $settingsNode.IdentityHandling
                IdentityColumn     = $settingsNode.IdentityColumn
                BatchColumn        = $settingsNode.BatchColumn
                ExistingDataAction = $settingsNode.ExistingDataAction
            }
        }

        # Parse column transformations
        foreach ($colNode in $mappingNode.Columns.Column) {
            $column = @{
                Type = $colNode.type
            }

            switch ($colNode.type) {
                "simple" {
                    $column.Source = $colNode.Source.column
                    $column.Target = $colNode.Target.column
                    $column.NullDefault = $colNode.Target.nullDefault
                }

                "concat" {
                    $column.Parts = @()
                    foreach ($part in $colNode.Source.Part) {
                        if ($part.column) {
                            $column.Parts += @{ Type = "column"; Value = $part.column }
                        } elseif ($part.literal) {
                            $column.Parts += @{ Type = "literal"; Value = $part.literal }
                        }
                    }
                    $column.Target = $colNode.Target.column
                    $column.NullDefault = $colNode.Target.nullDefault
                }

                "split" {
                    $column.Source = $colNode.Source.column
                    $column.Delimiter = $colNode.Delimiter.value
                    $column.Targets = @()
                    foreach ($part in $colNode.Targets.Part) {
                        $column.Targets += @{
                            Index  = [int]$part.index
                            Column = $part.column
                        }
                    }
                }

                "lookup" {
                    $column.Source = $colNode.Source.column
                    $column.Target = $colNode.Target.column
                    $column.NullDefault = $colNode.Target.nullDefault
                    $column.LookupTable = @{}
                    $column.LookupDefault = $null
                    foreach ($map in $colNode.LookupTable.Map) {
                        $column.LookupTable[$map.from] = $map.to
                    }
                    if ($colNode.LookupTable.Default) {
                        $column.LookupDefault = $colNode.LookupTable.Default.to
                    }
                }

                "calculated" {
                    $column.Expression = $colNode.Expression
                    $column.Target = $colNode.Target.column
                    $column.NullDefault = $colNode.Target.nullDefault
                }

                "static" {
                    $column.Target = $colNode.Target.column
                    if ($colNode.Value.function) {
                        $column.ValueType = "function"
                        $column.Value = $colNode.Value.function
                    } else {
                        $column.ValueType = "literal"
                        $column.Value = $colNode.Value.literal
                    }
                }

                "conditional" {
                    $column.Target = $colNode.Target.column
                    $column.Conditions = @()
                    foreach ($when in $colNode.When) {
                        $condition = @{
                            Test = $when.test
                        }
                        if ($when.Source) {
                            $condition.ValueType = "column"
                            $condition.Value = $when.Source.column
                        } else {
                            $condition.ValueType = "literal"
                            $condition.Value = $when.Value.literal
                        }
                        $column.Conditions += $condition
                    }
                    if ($colNode.Else) {
                        $column.Else = @{}
                        if ($colNode.Else.Source) {
                            $column.Else.ValueType = "column"
                            $column.Else.Value = $colNode.Else.Source.column
                        } else {
                            $column.Else.ValueType = "literal"
                            $column.Else.Value = $colNode.Else.Value.literal
                        }
                    }
                }

                "convert" {
                    $column.Source = $colNode.Source.column
                    $column.SourceFormat = $colNode.Source.format
                    $column.Target = $colNode.Target.column
                    $column.TargetType = $colNode.Target.type
                    $column.NullDefault = $colNode.Target.nullDefault
                }

                "keyLookup" {
                    $column.Source = $colNode.Source.column
                    $column.Target = $colNode.Target.column
                    $column.NullDefault = $colNode.Target.nullDefault
                    $column.KeyMapSourceTable = $colNode.KeyMap.sourceTable
                    $column.KeyMapSourceKeyColumn = $colNode.KeyMap.sourceKeyColumn
                }
            }

            $mapping.Columns += $column
        }

        Write-Log -Message "Table mapping loaded - $($mapping.SourceTable) -> $($mapping.TargetTable), Transforms: $($mapping.Columns.Count)" -Level Debug

        return $mapping
    }

    END {}
}