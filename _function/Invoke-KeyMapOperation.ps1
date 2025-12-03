Function Get-KeyMapTableName {
    <#
    .SYNOPSIS
        Generates a consistent temp table name for key mapping.
     
    .NOTES
        Name: Get-KeyMapTableName
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $tableName = Get-KeyMapTableName -SourceTable "SourceDB.Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $SourceTable
    )

    BEGIN {}

    PROCESS {
        # Replace dots with underscores for valid table name
        $safeName = $SourceTable -replace '\.', '_'
        $tableName = "_KeyMap_$safeName"

        return $tableName
    }

    END {}
}


Function New-KeyMapTable {
    <#
    .SYNOPSIS
        Creates a key mapping temp table in the target database.
     
    .NOTES
        Name: New-KeyMapTable
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        New-KeyMapTable -Connection $targetConnection -SourceTable "SourceDB.Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 60
    )

    BEGIN {}

    PROCESS {
        $tableName = Get-KeyMapTableName -SourceTable $SourceTable

        Write-Log -Message "Creating key map table [$tableName]..." -Level Verbose

        # Create table with OldKey and NewKey columns
        # Using NVARCHAR(450) to support various key types while allowing indexing
        $sql = @"
IF OBJECT_ID('dbo.$tableName', 'U') IS NOT NULL
    DROP TABLE dbo.$tableName;

CREATE TABLE dbo.$tableName (
    OldKey NVARCHAR(450) NOT NULL,
    NewKey NVARCHAR(450) NOT NULL,
    CONSTRAINT PK_$tableName PRIMARY KEY CLUSTERED (OldKey)
);

CREATE NONCLUSTERED INDEX IX_$($tableName)_NewKey ON dbo.$tableName (NewKey);
"@

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "Key map table [$tableName] created successfully" -Level Debug
        }
        catch {
            Write-Log -Message "Failed to create key map table [$tableName]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($command) { $command.Dispose() }
        }

        return $tableName
    }

    END {}
}


Function Add-KeyMapEntry {
    <#
    .SYNOPSIS
        Adds a key mapping entry to the temp table.
     
    .NOTES
        Name: Add-KeyMapEntry
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Add-KeyMapEntry -Connection $targetConnection -SourceTable "SourceDB.Customers" -OldKey "100" -NewKey "5001"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $true)]
        [string] $OldKey,

        [Parameter(Mandatory = $true)]
        [string] $NewKey,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $tableName = Get-KeyMapTableName -SourceTable $SourceTable

        $sql = "INSERT INTO dbo.$tableName (OldKey, NewKey) VALUES (@OldKey, @NewKey);"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $paramOld = $command.CreateParameter()
            $paramOld.ParameterName = "@OldKey"
            $paramOld.Value = $OldKey
            $command.Parameters.Add($paramOld) | Out-Null

            $paramNew = $command.CreateParameter()
            $paramNew.ParameterName = "@NewKey"
            $paramNew.Value = $NewKey
            $command.Parameters.Add($paramNew) | Out-Null

            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "Key map entry added: $OldKey -> $NewKey" -Level Debug
        }
        catch {
            Write-Log -Message "Failed to add key map entry: $OldKey -> $NewKey" -Level Warning -Exception $_.Exception
            # Don't throw - log and continue
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Add-KeyMapEntryBatch {
    <#
    .SYNOPSIS
        Adds multiple key mapping entries to the temp table in a single batch.
     
    .NOTES
        Name: Add-KeyMapEntryBatch
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Add-KeyMapEntryBatch -Connection $targetConnection -SourceTable "SourceDB.Customers" -KeyMappings @(@{OldKey="100"; NewKey="5001"}, @{OldKey="101"; NewKey="5002"})
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $true)]
        [array] $KeyMappings,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 120
    )

    BEGIN {}

    PROCESS {
        if ($KeyMappings.Count -eq 0) {
            return
        }

        $tableName = Get-KeyMapTableName -SourceTable $SourceTable

        Write-Log -Message "Adding $($KeyMappings.Count) key map entries to [$tableName]..." -Level Verbose

        # Build bulk insert using table-valued constructor (max 1000 rows per statement)
        $batchSize = 1000
        $batches = [System.Math]::Ceiling($KeyMappings.Count / $batchSize)

        for ($i = 0; $i -lt $batches; $i++) {
            $startIdx = $i * $batchSize
            $endIdx = [System.Math]::Min($startIdx + $batchSize, $KeyMappings.Count)
            $batchMappings = $KeyMappings[$startIdx..($endIdx - 1)]

            $values = @()
            foreach ($mapping in $batchMappings) {
                $oldKeyEscaped = $mapping.OldKey -replace "'", "''"
                $newKeyEscaped = $mapping.NewKey -replace "'", "''"
                $values += "('$oldKeyEscaped', '$newKeyEscaped')"
            }

            $sql = "INSERT INTO dbo.$tableName (OldKey, NewKey) VALUES $($values -join ', ');"

            try {
                $command = $Connection.CreateCommand()
                $command.CommandText = $sql
                $command.CommandTimeout = $TimeoutSeconds
                $command.ExecuteNonQuery() | Out-Null
            }
            catch {
                Write-Log -Message "Failed to add key map batch" -Level Error -Exception $_.Exception
                throw
            }
            finally {
                if ($command) { $command.Dispose() }
            }
        }

        Write-Log -Message "Key map entries added successfully" -Level Debug
    }

    END {}
}


Function Get-MappedKey {
    <#
    .SYNOPSIS
        Looks up a new key from the key mapping table.
     
    .NOTES
        Name: Get-MappedKey
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $newKey = Get-MappedKey -Connection $targetConnection -SourceTable "SourceDB.Customers" -OldKey "100"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $true)]
        [string] $OldKey,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $tableName = Get-KeyMapTableName -SourceTable $SourceTable

        $sql = "SELECT NewKey FROM dbo.$tableName WHERE OldKey = @OldKey;"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $param = $command.CreateParameter()
            $param.ParameterName = "@OldKey"
            $param.Value = $OldKey
            $command.Parameters.Add($param) | Out-Null

            $result = $command.ExecuteScalar()

            if ($null -eq $result) {
                Write-Log -Message "No key mapping found for OldKey: $OldKey in [$tableName]" -Level Debug
                return $null
            }

            return $result.ToString()
        }
        catch {
            Write-Log -Message "Failed to lookup key mapping for OldKey: $OldKey" -Level Warning -Exception $_.Exception
            return $null
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Get-AllKeyMappings {
    <#
    .SYNOPSIS
        Retrieves all key mappings from a key map table into a hashtable for fast lookup.
     
    .NOTES
        Name: Get-AllKeyMappings
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $keyMap = Get-AllKeyMappings -Connection $targetConnection -SourceTable "SourceDB.Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        $tableName = Get-KeyMapTableName -SourceTable $SourceTable

        Write-Log -Message "Loading all key mappings from [$tableName]..." -Level Verbose

        $sql = "SELECT OldKey, NewKey FROM dbo.$tableName;"
        $keyMap = @{}

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds

            $reader = $command.ExecuteReader()

            while ($reader.Read()) {
                $oldKey = $reader["OldKey"].ToString()
                $newKey = $reader["NewKey"].ToString()
                $keyMap[$oldKey] = $newKey
            }

            $reader.Close()

            Write-Log -Message "Loaded $($keyMap.Count) key mappings from [$tableName]" -Level Debug

            return $keyMap
        }
        catch {
            Write-Log -Message "Failed to load key mappings from [$tableName]" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($reader) { $reader.Dispose() }
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Remove-KeyMapTable {
    <#
    .SYNOPSIS
        Removes a key mapping temp table from the target database.
     
    .NOTES
        Name: Remove-KeyMapTable
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Remove-KeyMapTable -Connection $targetConnection -SourceTable "SourceDB.Customers"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $true)]
        [string] $SourceTable,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 60
    )

    BEGIN {}

    PROCESS {
        $tableName = Get-KeyMapTableName -SourceTable $SourceTable

        Write-Log -Message "Removing key map table [$tableName]..." -Level Verbose

        $sql = "IF OBJECT_ID('dbo.$tableName', 'U') IS NOT NULL DROP TABLE dbo.$tableName;"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "Key map table [$tableName] removed" -Level Debug
        }
        catch {
            Write-Log -Message "Failed to remove key map table [$tableName]" -Level Warning -Exception $_.Exception
            # Don't throw - cleanup should not fail the migration
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Remove-AllKeyMapTables {
    <#
    .SYNOPSIS
        Removes all key mapping temp tables from the target database.
     
    .NOTES
        Name: Remove-AllKeyMapTables
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Remove-AllKeyMapTables -Connection $targetConnection
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object] $Connection,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 120
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Removing all key map tables from target database..." -Level Info

        $sql = @"
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql += N'DROP TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + '.' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.tables
WHERE name LIKE '_KeyMap_%';

IF LEN(@sql) > 0
    EXEC sp_executesql @sql;
"@

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "All key map tables removed" -Level Info
        }
        catch {
            Write-Log -Message "Failed to remove all key map tables" -Level Warning -Exception $_.Exception
            # Don't throw - cleanup should not fail the migration
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}