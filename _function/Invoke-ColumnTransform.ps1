Function Invoke-ColumnTransform {
    <#
    .SYNOPSIS
        Applies a column transformation to a source row value.
     
    .NOTES
        Name: Invoke-ColumnTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $result = Invoke-ColumnTransform -Transformation $transform -SourceRow $row -KeyMaps $keyMaps
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow,

        [Parameter(Mandatory = $false)]
        [hashtable] $KeyMaps = @{},

        [Parameter(Mandatory = $false)]
        [string] $NullDefault = $null
    )

    BEGIN {}

    PROCESS {
        $type = $Transformation.Type
        $result = @{}

        switch ($type) {
            "simple" {
                $result = Invoke-SimpleTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "concat" {
                $result = Invoke-ConcatTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "split" {
                $result = Invoke-SplitTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "lookup" {
                $result = Invoke-LookupTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "calculated" {
                $result = Invoke-CalculatedTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "static" {
                $result = Invoke-StaticTransform -Transformation $Transformation
            }

            "conditional" {
                $result = Invoke-ConditionalTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "convert" {
                $result = Invoke-ConvertTransform -Transformation $Transformation -SourceRow $SourceRow
            }

            "keyLookup" {
                $result = Invoke-KeyLookupTransform -Transformation $Transformation -SourceRow $SourceRow -KeyMaps $KeyMaps
            }

            default {
                Write-Log -Message "Unknown transformation type: $type" -Level Warning
                $result = @{}
            }
        }

        return $result
    }

    END {}
}


Function Invoke-SimpleTransform {
    <#
    .SYNOPSIS
        Applies a simple column-to-column mapping.
     
    .NOTES
        Name: Invoke-SimpleTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $sourceColumn = $Transformation.Source
        $targetColumn = $Transformation.Target
        $nullDefault = $Transformation.NullDefault

        $value = $SourceRow[$sourceColumn]

        if ($null -eq $value -or $value -is [System.DBNull]) {
            $value = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value }
        }

        return @{ $targetColumn = $value }
    }

    END {}
}


Function Invoke-ConcatTransform {
    <#
    .SYNOPSIS
        Concatenates multiple source columns and/or literals into one target column.
     
    .NOTES
        Name: Invoke-ConcatTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $targetColumn = $Transformation.Target
        $nullDefault = $Transformation.NullDefault
        $parts = $Transformation.Parts

        $concatenated = ""
        $hasNull = $false

        foreach ($part in $parts) {
            if ($part.Type -eq "column") {
                $value = $SourceRow[$part.Value]
                if ($null -eq $value -or $value -is [System.DBNull]) {
                    $hasNull = $true
                    $concatenated += ""
                } else {
                    $concatenated += $value.ToString()
                }
            } elseif ($part.Type -eq "literal") {
                $concatenated += $part.Value
            }
        }

        # If all parts were null, return null or default
        if ([string]::IsNullOrEmpty($concatenated.Trim()) -and $hasNull) {
            $concatenated = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value }
        }

        return @{ $targetColumn = $concatenated }
    }

    END {}
}


Function Invoke-SplitTransform {
    <#
    .SYNOPSIS
        Splits a source column into multiple target columns by delimiter.
     
    .NOTES
        Name: Invoke-SplitTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $sourceColumn = $Transformation.Source
        $delimiter = $Transformation.Delimiter
        $targets = $Transformation.Targets

        $result = @{}
        $value = $SourceRow[$sourceColumn]

        if ($null -eq $value -or $value -is [System.DBNull]) {
            # Set all target columns to null
            foreach ($target in $targets) {
                $result[$target.Column] = [System.DBNull]::Value
            }
        } else {
            $parts = $value.ToString().Split($delimiter)

            foreach ($target in $targets) {
                $index = $target.Index
                $column = $target.Column

                if ($index -lt $parts.Count) {
                    $result[$column] = $parts[$index].Trim()
                } else {
                    $result[$column] = [System.DBNull]::Value
                }
            }
        }

        return $result
    }

    END {}
}


Function Invoke-LookupTransform {
    <#
    .SYNOPSIS
        Maps a source value to a target value using an inline lookup table.
     
    .NOTES
        Name: Invoke-LookupTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $sourceColumn = $Transformation.Source
        $targetColumn = $Transformation.Target
        $lookupTable = $Transformation.LookupTable
        $lookupDefault = $Transformation.LookupDefault
        $nullDefault = $Transformation.NullDefault

        $value = $SourceRow[$sourceColumn]

        if ($null -eq $value -or $value -is [System.DBNull]) {
            $mappedValue = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value }
        } else {
            $sourceValue = $value.ToString()

            if ($lookupTable.ContainsKey($sourceValue)) {
                $mappedValue = $lookupTable[$sourceValue]
            } elseif ($null -ne $lookupDefault) {
                $mappedValue = $lookupDefault
            } else {
                $mappedValue = [System.DBNull]::Value
                Write-Log -Message "Lookup value not found for '$sourceValue' in column $sourceColumn" -Level Debug
            }
        }

        return @{ $targetColumn = $mappedValue }
    }

    END {}
}


Function Invoke-CalculatedTransform {
    <#
    .SYNOPSIS
        Evaluates a calculated expression using source column values.
     
    .NOTES
        Name: Invoke-CalculatedTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $expression = $Transformation.Expression
        $targetColumn = $Transformation.Target
        $nullDefault = $Transformation.NullDefault

        # Replace column references with actual values
        $evalExpression = $expression

        foreach ($key in $SourceRow.Keys) {
            $value = $SourceRow[$key]

            if ($null -eq $value -or $value -is [System.DBNull]) {
                # If any value in the expression is null, result is null
                return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
            }

            # Replace column name with value (handle numeric values)
            if ($value -is [string]) {
                $evalExpression = $evalExpression -replace "\b$key\b", "'$value'"
            } else {
                $evalExpression = $evalExpression -replace "\b$key\b", $value.ToString()
            }
        }

        try {
            # Evaluate the expression
            $result = Invoke-Expression $evalExpression
            return @{ $targetColumn = $result }
        }
        catch {
            Write-Log -Message "Failed to evaluate expression: $expression" -Level Warning -Exception $_.Exception
            return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
        }
    }

    END {}
}


Function Invoke-StaticTransform {
    <#
    .SYNOPSIS
        Returns a static/hardcoded value or SQL function result.
     
    .NOTES
        Name: Invoke-StaticTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation
    )

    BEGIN {}

    PROCESS {
        $targetColumn = $Transformation.Target
        $valueType = $Transformation.ValueType
        $value = $Transformation.Value

        if ($valueType -eq "literal") {
            return @{ $targetColumn = $value }
        } elseif ($valueType -eq "function") {
            # Handle common SQL functions in PowerShell
            switch -Regex ($value.ToUpper()) {
                "GETDATE\(\)" {
                    return @{ $targetColumn = (Get-Date) }
                }
                "GETUTCDATE\(\)" {
                    return @{ $targetColumn = (Get-Date).ToUniversalTime() }
                }
                "NEWID\(\)" {
                    return @{ $targetColumn = [System.Guid]::NewGuid().ToString() }
                }
                "SUSER_SNAME\(\)" {
                    return @{ $targetColumn = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name }
                }
                default {
                    # For other functions, store as string - will need SQL evaluation
                    Write-Log -Message "SQL function '$value' not evaluated in PowerShell, storing as marker" -Level Debug
                    return @{ $targetColumn = "__SQL_FUNC:$value" }
                }
            }
        }

        return @{ $targetColumn = [System.DBNull]::Value }
    }

    END {}
}


Function Invoke-ConditionalTransform {
    <#
    .SYNOPSIS
        Applies conditional logic to determine the target value.
     
    .NOTES
        Name: Invoke-ConditionalTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $targetColumn = $Transformation.Target
        $conditions = $Transformation.Conditions
        $elseClause = $Transformation.Else

        foreach ($condition in $conditions) {
            $test = $condition.Test

            # Parse the test expression (e.g., "AccountType = 'B'")
            if (Test-Condition -Test $test -SourceRow $SourceRow) {
                if ($condition.ValueType -eq "column") {
                    $value = $SourceRow[$condition.Value]
                    return @{ $targetColumn = $value }
                } else {
                    return @{ $targetColumn = $condition.Value }
                }
            }
        }

        # No conditions matched, use Else clause
        if ($elseClause) {
            if ($elseClause.ValueType -eq "column") {
                $value = $SourceRow[$elseClause.Value]
                return @{ $targetColumn = $value }
            } else {
                return @{ $targetColumn = $elseClause.Value }
            }
        }

        return @{ $targetColumn = [System.DBNull]::Value }
    }

    END {}
}


Function Test-Condition {
    <#
    .SYNOPSIS
        Evaluates a condition test expression against a source row.
     
    .NOTES
        Name: Test-Condition
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string] $Test,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        # Parse simple conditions: "Column = 'Value'" or "Column = Value"
        # Supports: =, !=, <>, <, >, <=, >=, LIKE, IS NULL, IS NOT NULL

        $test = $Test.Trim()

        # Handle IS NULL
        if ($test -match "^(\w+)\s+IS\s+NULL$") {
            $column = $Matches[1]
            $value = $SourceRow[$column]
            return ($null -eq $value -or $value -is [System.DBNull])
        }

        # Handle IS NOT NULL
        if ($test -match "^(\w+)\s+IS\s+NOT\s+NULL$") {
            $column = $Matches[1]
            $value = $SourceRow[$column]
            return ($null -ne $value -and $value -isnot [System.DBNull])
        }

        # Handle comparison operators
        if ($test -match "^(\w+)\s*(=|!=|<>|<=|>=|<|>|LIKE)\s*'?([^']*)'?$") {
            $column = $Matches[1]
            $operator = $Matches[2]
            $compareValue = $Matches[3]

            $sourceValue = $SourceRow[$column]

            if ($null -eq $sourceValue -or $sourceValue -is [System.DBNull]) {
                return $false
            }

            $sourceValueStr = $sourceValue.ToString()

            switch ($operator) {
                "=" { return $sourceValueStr -eq $compareValue }
                "!=" { return $sourceValueStr -ne $compareValue }
                "<>" { return $sourceValueStr -ne $compareValue }
                "<" { return [double]$sourceValueStr -lt [double]$compareValue }
                ">" { return [double]$sourceValueStr -gt [double]$compareValue }
                "<=" { return [double]$sourceValueStr -le [double]$compareValue }
                ">=" { return [double]$sourceValueStr -ge [double]$compareValue }
                "LIKE" {
                    $pattern = $compareValue -replace '%', '.*' -replace '_', '.'
                    return $sourceValueStr -match "^$pattern$"
                }
            }
        }

        Write-Log -Message "Could not parse condition: $Test" -Level Warning
        return $false
    }

    END {}
}


Function Invoke-ConvertTransform {
    <#
    .SYNOPSIS
        Converts a source value to a target data type with explicit format.
     
    .NOTES
        Name: Invoke-ConvertTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $sourceColumn = $Transformation.Source
        $sourceFormat = $Transformation.SourceFormat
        $targetColumn = $Transformation.Target
        $targetType = $Transformation.TargetType
        $nullDefault = $Transformation.NullDefault

        $value = $SourceRow[$sourceColumn]

        if ($null -eq $value -or $value -is [System.DBNull]) {
            return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
        }

        try {
            $convertedValue = $null

            switch ($targetType.ToLower()) {
                { $_ -in "datetime", "datetime2", "date", "smalldatetime" } {
                    if ($sourceFormat) {
                        $convertedValue = [datetime]::ParseExact($value.ToString(), $sourceFormat, [System.Globalization.CultureInfo]::InvariantCulture)
                    } else {
                        $convertedValue = [datetime]::Parse($value.ToString())
                    }
                }

                "int" {
                    $convertedValue = [int]$value
                }

                "bigint" {
                    $convertedValue = [long]$value
                }

                "decimal" {
                    $convertedValue = [decimal]$value
                }

                "float" {
                    $convertedValue = [double]$value
                }

                "bit" {
                    $convertedValue = [bool]$value
                }

                { $_ -in "varchar", "nvarchar", "char", "nchar", "text", "ntext" } {
                    $convertedValue = $value.ToString()
                }

                "uniqueidentifier" {
                    $convertedValue = [System.Guid]::Parse($value.ToString())
                }

                default {
                    $convertedValue = $value
                }
            }

            return @{ $targetColumn = $convertedValue }
        }
        catch {
            Write-Log -Message "Failed to convert value '$value' to $targetType for column $sourceColumn" -Level Warning -Exception $_.Exception
            return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
        }
    }

    END {}
}


Function Invoke-KeyLookupTransform {
    <#
    .SYNOPSIS
        Looks up a new key from a key mapping table for foreign key translation.
     
    .NOTES
        Name: Invoke-KeyLookupTransform
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Transformation,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow,

        [Parameter(Mandatory = $false)]
        [hashtable] $KeyMaps = @{}
    )

    BEGIN {}

    PROCESS {
        $sourceColumn = $Transformation.Source
        $targetColumn = $Transformation.Target
        $nullDefault = $Transformation.NullDefault
        $keyMapSourceTable = $Transformation.KeyMapSourceTable
        $keyMapSourceKeyColumn = $Transformation.KeyMapSourceKeyColumn

        $value = $SourceRow[$sourceColumn]

        if ($null -eq $value -or $value -is [System.DBNull]) {
            return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
        }

        $oldKey = $value.ToString()

        # Look up in the key map
        $keyMap = $KeyMaps[$keyMapSourceTable]

        if (-not $keyMap) {
            Write-Log -Message "Key map not found for source table: $keyMapSourceTable" -Level Warning
            return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
        }

        if ($keyMap.ContainsKey($oldKey)) {
            $newKey = $keyMap[$oldKey]
            return @{ $targetColumn = $newKey }
        } else {
            Write-Log -Message "Key mapping not found for OldKey: $oldKey in $keyMapSourceTable" -Level Debug
            return @{ $targetColumn = if ($nullDefault) { $nullDefault } else { [System.DBNull]::Value } }
        }
    }

    END {}
}


Function Invoke-SimpleColumnMapping {
    <#
    .SYNOPSIS
        Applies a simple column mapping from MasterConfig (source -> target with optional format).
     
    .NOTES
        Name: Invoke-SimpleColumnMapping
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable] $Mapping,

        [Parameter(Mandatory = $true)]
        [hashtable] $SourceRow
    )

    BEGIN {}

    PROCESS {
        $sourceColumn = $Mapping.Source
        $targetColumn = $Mapping.Target
        $sourceFormat = $Mapping.SourceFormat

        $value = $SourceRow[$sourceColumn]

        if ($null -eq $value -or $value -is [System.DBNull]) {
            return @{ $targetColumn = [System.DBNull]::Value }
        }

        # Apply date format conversion if specified
        if ($sourceFormat) {
            try {
                $value = [datetime]::ParseExact($value.ToString(), $sourceFormat, [System.Globalization.CultureInfo]::InvariantCulture)
            }
            catch {
                Write-Log -Message "Failed to parse date '$value' with format '$sourceFormat'" -Level Warning
                # Return original value if parsing fails
            }
        }

        return @{ $targetColumn = $value }
    }

    END {}
}