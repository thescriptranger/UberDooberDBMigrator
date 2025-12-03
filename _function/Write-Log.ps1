Function Write-Log {
    <#
    .SYNOPSIS
        Writes a log message at the specified level.
     
    .NOTES
        Name: Write-Log
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Write-Log -Message "Migration started" -Level Info
     
    .EXAMPLE
        Write-Log -Message "Connection failed" -Level Error -Exception $_.Exception
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $Message,

        [Parameter(Mandatory = $false, Position = 1)]
        [ValidateSet("Error", "Warning", "Info", "Verbose", "Debug")]
        [string] $Level = "Info",

        [Parameter(Mandatory = $false)]
        [System.Exception] $Exception,

        [Parameter(Mandatory = $false)]
        [string] $Table
    )

    BEGIN {}

    PROCESS {
        # Log level hierarchy (lower number = higher priority)
        $levelPriority = @{
            "Error"   = 1
            "Warning" = 2
            "Info"    = 3
            "Verbose" = 4
            "Debug"   = 5
        }

        # Get current log level from script scope (default to Info)
        $currentLogLevel = if ($script:LogLevel) { $script:LogLevel } else { "Info" }
        $currentPriority = $levelPriority[$currentLogLevel]
        $messagePriority = $levelPriority[$Level]

        # Only log if message priority is equal or higher than current level
        if ($messagePriority -gt $currentPriority) {
            return
        }

        # Build log entry
        $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
        $tablePrefix = if ($Table) { "[$Table] " } else { "" }
        $logEntry = "[$timestamp] [$Level] $tablePrefix$Message"

        # Add exception details if provided
        if ($Exception) {
            $logEntry += "`n    Exception: $($Exception.Message)"
            if ($Exception.InnerException) {
                $logEntry += "`n    Inner Exception: $($Exception.InnerException.Message)"
            }
        }

        # Write to console with appropriate color
        switch ($Level) {
            "Error"   { Write-Host $logEntry -ForegroundColor Red }
            "Warning" { Write-Host $logEntry -ForegroundColor Yellow }
            "Info"    { Write-Host $logEntry -ForegroundColor White }
            "Verbose" { Write-Host $logEntry -ForegroundColor Cyan }
            "Debug"   { Write-Host $logEntry -ForegroundColor Gray }
        }

        # Write to transcript (automatically captured by Start-Transcript)
        # No additional action needed as Write-Host is captured
    }

    END {}
}