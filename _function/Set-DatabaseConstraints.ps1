Function Disable-DatabaseConstraints {
    <#
    .SYNOPSIS
        Disables all foreign key constraints and check constraints on the target database.
     
    .NOTES
        Name: Disable-DatabaseConstraints
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Disable-DatabaseConstraints -Connection $targetConnection
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [object] $Connection,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Disabling all foreign key and check constraints on target database..." -Level Info

        $sql = @"
DECLARE @sql NVARCHAR(MAX) = N'';

-- Disable all foreign key constraints
SELECT @sql += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + ' NOCHECK CONSTRAINT ' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.foreign_keys
WHERE is_disabled = 0;

-- Disable all check constraints
SELECT @sql += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + ' NOCHECK CONSTRAINT ' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.check_constraints
WHERE is_disabled = 0;

EXEC sp_executesql @sql;
"@

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "All constraints disabled successfully" -Level Info
        }
        catch {
            Write-Log -Message "Failed to disable constraints" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Enable-DatabaseConstraints {
    <#
    .SYNOPSIS
        Re-enables all foreign key constraints and check constraints on the target database.
     
    .NOTES
        Name: Enable-DatabaseConstraints
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Enable-DatabaseConstraints -Connection $targetConnection
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [object] $Connection,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Re-enabling all foreign key and check constraints on target database..." -Level Info

        $sql = @"
DECLARE @sql NVARCHAR(MAX) = N'';

-- Re-enable all foreign key constraints
SELECT @sql += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + ' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.foreign_keys
WHERE is_disabled = 1;

-- Re-enable all check constraints
SELECT @sql += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id)) + ' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.check_constraints
WHERE is_disabled = 1;

EXEC sp_executesql @sql;
"@

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "All constraints re-enabled successfully" -Level Info
        }
        catch {
            Write-Log -Message "Failed to re-enable constraints" -Level Error -Exception $_.Exception
            throw
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Disable-TableTriggers {
    <#
    .SYNOPSIS
        Disables all triggers on a specific table.
     
    .NOTES
        Name: Disable-TableTriggers
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Disable-TableTriggers -Connection $targetConnection -Schema "dbo" -Table "Customers"
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
        [int] $TimeoutSeconds = 60
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Disabling triggers on [$Schema].[$Table]..." -Level Verbose

        $sql = "DISABLE TRIGGER ALL ON [$Schema].[$Table];"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "Triggers disabled on [$Schema].[$Table]" -Level Debug
        }
        catch {
            Write-Log -Message "Failed to disable triggers on [$Schema].[$Table]" -Level Warning -Exception $_.Exception
            # Don't throw - triggers may not exist on all tables
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Enable-TableTriggers {
    <#
    .SYNOPSIS
        Re-enables all triggers on a specific table.
     
    .NOTES
        Name: Enable-TableTriggers
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Enable-TableTriggers -Connection $targetConnection -Schema "dbo" -Table "Customers"
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
        [int] $TimeoutSeconds = 60
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Re-enabling triggers on [$Schema].[$Table]..." -Level Verbose

        $sql = "ENABLE TRIGGER ALL ON [$Schema].[$Table];"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "Triggers re-enabled on [$Schema].[$Table]" -Level Debug
        }
        catch {
            Write-Log -Message "Failed to re-enable triggers on [$Schema].[$Table]" -Level Warning -Exception $_.Exception
            # Don't throw - triggers may not exist on all tables
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Set-IdentityInsert {
    <#
    .SYNOPSIS
        Enables or disables IDENTITY_INSERT on a specific table.
     
    .NOTES
        Name: Set-IdentityInsert
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Set-IdentityInsert -Connection $targetConnection -Schema "dbo" -Table "Customers" -Enable $true
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
        [bool] $Enable,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 60
    )

    BEGIN {}

    PROCESS {
        $state = if ($Enable) { "ON" } else { "OFF" }
        Write-Log -Message "Setting IDENTITY_INSERT $state for [$Schema].[$Table]..." -Level Verbose

        $sql = "SET IDENTITY_INSERT [$Schema].[$Table] $state;"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "IDENTITY_INSERT set to $state for [$Schema].[$Table]" -Level Debug
        }
        catch {
            # This may fail if the table doesn't have an identity column - that's OK
            Write-Log -Message "Could not set IDENTITY_INSERT on [$Schema].[$Table] (table may not have identity column)" -Level Debug
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}


Function Truncate-TargetTable {
    <#
    .SYNOPSIS
        Truncates a target table before migration.
     
    .NOTES
        Name: Truncate-TargetTable
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Truncate-TargetTable -Connection $targetConnection -Schema "dbo" -Table "Customers"
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
        [int] $TimeoutSeconds = 300
    )

    BEGIN {}

    PROCESS {
        Write-Log -Message "Truncating target table [$Schema].[$Table]..." -Level Info

        # Try TRUNCATE first, fall back to DELETE if foreign keys prevent truncation
        $sql = "TRUNCATE TABLE [$Schema].[$Table];"

        try {
            $command = $Connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = $TimeoutSeconds
            $command.ExecuteNonQuery() | Out-Null

            Write-Log -Message "Table [$Schema].[$Table] truncated successfully" -Level Info
        }
        catch {
            Write-Log -Message "TRUNCATE failed, attempting DELETE..." -Level Warning

            try {
                $command.CommandText = "DELETE FROM [$Schema].[$Table];"
                $command.ExecuteNonQuery() | Out-Null

                Write-Log -Message "Table [$Schema].[$Table] cleared via DELETE" -Level Info
            }
            catch {
                Write-Log -Message "Failed to clear table [$Schema].[$Table]" -Level Error -Exception $_.Exception
                throw
            }
        }
        finally {
            if ($command) { $command.Dispose() }
        }
    }

    END {}
}