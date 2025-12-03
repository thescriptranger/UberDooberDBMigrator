Function New-DataTable {
    <#
    .SYNOPSIS
        Creates a DataTable with columns matching the target table schema.
     
    .NOTES
        Name: New-DataTable
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $dataTable = New-DataTable -ColumnNames @("CustomerID", "FullName", "Email")
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array] $ColumnNames
    )

    BEGIN {}

    PROCESS {
        $dataTable = New-Object System.Data.DataTable

        foreach ($columnName in $ColumnNames) {
            $column = New-Object System.Data.DataColumn($columnName, [object])
            $dataTable.Columns.Add($column) | Out-Null
        }

        Write-Log -Message "DataTable created with $($ColumnNames.Count) columns" -Level Debug

        return $dataTable
    }

    END {}
}


Function Add-DataTableRow {
    <#
    .SYNOPSIS
        Adds a row to the DataTable from a transformed row hashtable.
     
    .NOTES
        Name: Add-DataTableRow
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Add-DataTableRow -DataTable $dt -RowData $transformedRow
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [System.Data.DataTable] $DataTable,

        [Parameter(Mandatory = $true)]
        [hashtable] $RowData
    )

    BEGIN {}

    PROCESS {
        $row = $DataTable.NewRow()

        foreach ($column in $DataTable.Columns) {
            $columnName = $column.ColumnName

            if ($RowData.ContainsKey($columnName)) {
                $value = $RowData[$columnName]

                if ($null -eq $value) {
                    $row[$columnName] = [System.DBNull]::Value
                } else {
                    $row[$columnName] = $value
                }
            } else {
                $row[$columnName] = [System.DBNull]::Value
            }
        }

        $DataTable.Rows.Add($row) | Out-Null
    }

    END {}
}


Function Get-TargetTableColumns {
    <#
    .SYNOPSIS
        Gets the list of column names from the target table.
     
    .NOTES
        Name: Get-TargetTableColumns
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $columns = Get-TargetTableColumns -Connection $conn -Schema "dbo" -Table "Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $Schema,

        [Parameter(Mandatory = $true)]
        [string] $Table,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $sql = @"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table
ORDER BY ORDINAL_POSITION;
"@

        $columns = @()

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $paramSchema = $command.CreateParameter()
            $paramSchema.ParameterName = "@Schema"
            $paramSchema.Value = $Schema
            $command.Parameters.Add($paramSchema) | Out-Null

            $paramTable = $command.CreateParameter()
            $paramTable.ParameterName = "@Table"
            $paramTable.Value = $Table
            $command.Parameters.Add($paramTable) | Out-Null

            $reader = $command.ExecuteReader()

            while ($reader.Read()) {
                $columns += $reader["COLUMN_NAME"].ToString()
            }

            $reader.Close()

            Write-Log -Message "Retrieved $($columns.Count) columns from [$Schema].[$Table]" -Level Debug

            return $columns
        }
        catch {
            Write-Log -Message "Failed to get columns from [$Schema].[$Table]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($reader) { $reader.Dispose() }
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Get-TargetIdentityColumn {
    <#
    .SYNOPSIS
        Gets the identity column name from the target table, if any.
     
    .NOTES
        Name: Get-TargetIdentityColumn
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $identityCol = Get-TargetIdentityColumn -Connection $conn -Schema "dbo" -Table "Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $Schema,

        [Parameter(Mandatory = $true)]
        [string] $Table,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $sql = @"
SELECT c.name AS ColumnName
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @Schema AND t.name = @Table AND c.is_identity = 1;
"@

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $paramSchema = $command.CreateParameter()
            $paramSchema.ParameterName = "@Schema"
            $paramSchema.Value = $Schema
            $command.Parameters.Add($paramSchema) | Out-Null

            $paramTable = $command.CreateParameter()
            $paramTable.ParameterName = "@Table"
            $paramTable.Value = $Table
            $command.Parameters.Add($paramTable) | Out-Null

            $result = $command.ExecuteScalar()

            if ($result) {
                Write-Log -Message "Identity column found: $result in [$Schema].[$Table]" -Level Debug
                return $result.ToString()
            }

            return $null
        }
        catch {
            Write-Log -Message "Failed to check for identity column in [$Schema].[$Table]" -Level Warning -Exception $_.Exception
            return $null
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Invoke-BatchInsert {
    <#
    .SYNOPSIS
        Bulk inserts a DataTable into the target table using SqlBulkCopy.
     
    .NOTES
        Name: Invoke-BatchInsert
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Invoke-BatchInsert -Connection $conn -Schema "dbo" -Table "Customers" -DataTable $dt -TimeoutSeconds 300
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
        [System.Data.DataTable] $DataTable,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        $destinationTable = "[$Schema].[$Table]"
        $rowCount = $DataTable.Rows.Count

        Write-Log -Message "Bulk inserting $rowCount rows into $destinationTable..." -Level Verbose

        try {
            $bulkCopy = New-Object Microsoft.Data.SqlClient.SqlBulkCopy($Connection)
            $bulkCopy.DestinationTableName = $destinationTable
            $bulkCopy.BulkCopyTimeout = $TimeoutSeconds
            $bulkCopy.BatchSize = $rowCount

            # Map columns explicitly
            foreach ($column in $DataTable.Columns) {
                $bulkCopy.ColumnMappings.Add($column.ColumnName, $column.ColumnName) | Out-Null
            }

            $bulkCopy.WriteToServer($DataTable)

            Write-Log -Message "Bulk insert completed: $rowCount rows inserted into $destinationTable" -Level Debug

            return $rowCount
        }
        catch {
            Write-Log -Message "Bulk insert failed for $destinationTable" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($bulkCopy) { $bulkCopy.Close() }
        }
    }

    END {}
}


Function Invoke-SingleRowInsert {
    <#
    .SYNOPSIS
        Inserts a single row into the target table and returns the new identity value if applicable.
     
    .NOTES
        Name: Invoke-SingleRowInsert
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $newId = Invoke-SingleRowInsert -Connection $conn -Schema "dbo" -Table "Customers" -RowData $row -IdentityColumn "CustomerID"
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
        [hashtable] $RowData,

        [Parameter(Mandatory = $false)]
        [string] $IdentityColumn,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $columns = @()
        $paramNames = @()
        $paramIndex = 0

        foreach ($key in $RowData.Keys) {
            # Skip identity column if generating new identity
            if ($IdentityColumn -and $key -eq $IdentityColumn) {
                continue
            }

            $columns += "[$key]"
            $paramNames += "@p$paramIndex"
            $paramIndex++
        }

        $columnList = $columns -join ", "
        $paramList = $paramNames -join ", "

        $sql = "INSERT INTO [$Schema].[$Table] ($columnList) VALUES ($paramList)"

        if ($IdentityColumn) {
            $sql += "; SELECT SCOPE_IDENTITY();"
        }

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $paramIndex = 0
            foreach ($key in $RowData.Keys) {
                if ($IdentityColumn -and $key -eq $IdentityColumn) {
                    continue
                }

                $param = $command.CreateParameter()
                $param.ParameterName = "@p$paramIndex"
                $value = $RowData[$key]

                if ($null -eq $value -or $value -is [System.DBNull]) {
                    $param.Value = [System.DBNull]::Value
                } else {
                    $param.Value = $value
                }

                $command.Parameters.Add($param) | Out-Null
                $paramIndex++
            }

            if ($IdentityColumn) {
                $newId = $command.ExecuteScalar()
                return $newId
            } else {
                $command.ExecuteNonQuery() | Out-Null
                return $null
            }
        }
        catch {
            Write-Log -Message "Single row insert failed for [$Schema].[$Table]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Invoke-BatchInsertWithIdentityCapture {
    <#
    .SYNOPSIS
        Inserts rows one-by-one to capture new identity values for key mapping.
     
    .NOTES
        Name: Invoke-BatchInsertWithIdentityCapture
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $keyMappings = Invoke-BatchInsertWithIdentityCapture -Connection $conn -Schema "dbo" -Table "Customers" -Rows $rows -SourceKeyColumn "CustID" -IdentityColumn "CustomerID"
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
        [array] $Rows,

        [Parameter(Mandatory = $true)]
        [string] $SourceKeyColumn,

        [Parameter(Mandatory = $true)]
        [string] $IdentityColumn,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $keyMappings = @()
        $insertedCount = 0

        Write-Log -Message "Inserting $($Rows.Count) rows with identity capture into [$Schema].[$Table]..." -Level Verbose

        foreach ($row in $Rows) {
            # Get the original key value
            $oldKey = $row["__SourceKey"]

            if (-not $oldKey) {
                Write-Log -Message "Source key not found in row data" -Level Warning
                continue
            }

            try {
                $newId = Invoke-SingleRowInsert -Connection $Connection -Schema $Schema -Table $Table -RowData $row -IdentityColumn $IdentityColumn -TimeoutSeconds $TimeoutSeconds

                if ($newId) {
                    $keyMappings += @{
                        OldKey = $oldKey.ToString()
                        NewKey = $newId.ToString()
                    }
                }

                $insertedCount++
            }
            catch {
                Write-Log -Message "Failed to insert row with source key: $oldKey" -Level Warning -Exception $_.Exception
                throw
            }
        }

        Write-Log -Message "Inserted $insertedCount rows with identity capture" -Level Debug

        return $keyMappings
    }

    END {}
}


Function Invoke-BulkInsertWithIdentityPreserve {
    <#
    .SYNOPSIS
        Bulk inserts rows while preserving original identity values using IDENTITY_INSERT.
     
    .NOTES
        Name: Invoke-BulkInsertWithIdentityPreserve
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Invoke-BulkInsertWithIdentityPreserve -Connection $conn -Schema "dbo" -Table "Customers" -DataTable $dt -IdentityColumn "CustomerID"
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
        [System.Data.DataTable] $DataTable,

        [Parameter(Mandatory = $true)]
        [string] $IdentityColumn,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        $rowCount = $DataTable.Rows.Count

        Write-Log -Message "Bulk inserting $rowCount rows with identity preserve into [$Schema].[$Table]..." -Level Verbose

        try {
            # Enable identity insert
            Set-IdentityInsert -Connection $Connection -Schema $Schema -Table $Table -Enable $true

            # Perform bulk insert
            $inserted = Invoke-BatchInsert -Connection $Connection -Schema $Schema -Table $Table -DataTable $DataTable -TimeoutSeconds $TimeoutSeconds

            return $inserted
        }
        finally {
            # Always disable identity insert
            Set-IdentityInsert -Connection $Connection -Schema $Schema -Table $Table -Enable $false
        }
    }

    END {}
}