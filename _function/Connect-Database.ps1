Function Install-DatabaseProvider {
    <#
    .SYNOPSIS
        Installs required database provider assemblies if missing.
     
    .NOTES
        Name: Install-DatabaseProvider
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Install-DatabaseProvider -Provider "Oracle"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [ValidateSet("SqlServer", "AzureSql", "Oracle", "MySql", "PostgreSql")]
        [string] $Provider
    )

    BEGIN {}

    PROCESS {
        switch ($Provider) {
            { $_ -in "SqlServer", "AzureSql" } {
                # Check for Microsoft.Data.SqlClient
                $sqlClientPath = Get-Package -Name "Microsoft.Data.SqlClient" -ErrorAction SilentlyContinue
                if (-not $sqlClientPath) {
                    Write-Log -Message "Installing Microsoft.Data.SqlClient..." -Level Info
                    Install-Package -Name "Microsoft.Data.SqlClient" -Source "nuget.org" -ProviderName NuGet -Scope CurrentUser -Force -SkipDependencies | Out-Null
                }
                
                # Load the assembly
                try {
                    Add-Type -Path "$env:USERPROFILE\.nuget\packages\microsoft.data.sqlclient\*\lib\netstandard2.1\Microsoft.Data.SqlClient.dll" -ErrorAction SilentlyContinue
                } catch {
                    # Try loading from GAC or already loaded
                    [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.Data.SqlClient") | Out-Null
                }

                # For Azure AD authentication, ensure Azure.Identity is available
                if ($Provider -eq "AzureSql") {
                    $azureIdentity = Get-Package -Name "Azure.Identity" -ErrorAction SilentlyContinue
                    if (-not $azureIdentity) {
                        Write-Log -Message "Installing Azure.Identity for Azure AD authentication..." -Level Info
                        Install-Package -Name "Azure.Identity" -Source "nuget.org" -ProviderName NuGet -Scope CurrentUser -Force -SkipDependencies | Out-Null
                    }
                }
            }

            "Oracle" {
                $oraclePackage = Get-Package -Name "Oracle.ManagedDataAccess.Core" -ErrorAction SilentlyContinue
                if (-not $oraclePackage) {
                    Write-Log -Message "Installing Oracle.ManagedDataAccess.Core..." -Level Info
                    Install-Package -Name "Oracle.ManagedDataAccess.Core" -Source "nuget.org" -ProviderName NuGet -Scope CurrentUser -Force | Out-Null
                }
                
                try {
                    Add-Type -Path "$env:USERPROFILE\.nuget\packages\oracle.manageddataaccess.core\*\lib\netstandard2.1\Oracle.ManagedDataAccess.dll" -ErrorAction SilentlyContinue
                } catch {
                    [System.Reflection.Assembly]::LoadWithPartialName("Oracle.ManagedDataAccess") | Out-Null
                }
            }

            "MySql" {
                $mySqlPackage = Get-Package -Name "MySql.Data" -ErrorAction SilentlyContinue
                if (-not $mySqlPackage) {
                    Write-Log -Message "Installing MySql.Data..." -Level Info
                    Install-Package -Name "MySql.Data" -Source "nuget.org" -ProviderName NuGet -Scope CurrentUser -Force | Out-Null
                }
                
                try {
                    Add-Type -Path "$env:USERPROFILE\.nuget\packages\mysql.data\*\lib\netstandard2.1\MySql.Data.dll" -ErrorAction SilentlyContinue
                } catch {
                    [System.Reflection.Assembly]::LoadWithPartialName("MySql.Data") | Out-Null
                }
            }

            "PostgreSql" {
                $npgsqlPackage = Get-Package -Name "Npgsql" -ErrorAction SilentlyContinue
                if (-not $npgsqlPackage) {
                    Write-Log -Message "Installing Npgsql..." -Level Info
                    Install-Package -Name "Npgsql" -Source "nuget.org" -ProviderName NuGet -Scope CurrentUser -Force | Out-Null
                }
                
                try {
                    Add-Type -Path "$env:USERPROFILE\.nuget\packages\npgsql\*\lib\netstandard2.1\Npgsql.dll" -ErrorAction SilentlyContinue
                } catch {
                    [System.Reflection.Assembly]::LoadWithPartialName("Npgsql") | Out-Null
                }
            }
        }

        Write-Log -Message "Database provider $Provider is ready" -Level Verbose
    }

    END {}
}


Function Get-PromptedCredentials {
    <#
    .SYNOPSIS
        Prompts user for missing credentials.
     
    .NOTES
        Name: Get-PromptedCredentials
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $creds = Get-PromptedCredentials -ConnectionConfig $config -ConnectionName "Source"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [hashtable] $ConnectionConfig,

        [Parameter(Mandatory = $true, Position = 1)]
        [string] $ConnectionName
    )

    BEGIN {}

    PROCESS {
        $userId = $ConnectionConfig.UserId
        $password = $ConnectionConfig.Password

        # Only prompt for SqlAuth
        if ($ConnectionConfig.AuthType -eq "SqlAuth") {
            if ([string]::IsNullOrWhiteSpace($userId)) {
                $userId = Read-Host "Enter User ID for $ConnectionName connection ($($ConnectionConfig.Server))"
            }

            if ([string]::IsNullOrWhiteSpace($password)) {
                $securePassword = Read-Host "Enter Password for $ConnectionName connection ($($ConnectionConfig.Server))" -AsSecureString
                $password = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
                )
            }
        }

        return @{
            UserId   = $userId
            Password = $password
        }
    }

    END {}
}


Function Build-ConnectionString {
    <#
    .SYNOPSIS
        Builds a connection string for the specified provider and authentication type.
     
    .NOTES
        Name: Build-ConnectionString
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $connStr = Build-ConnectionString -ConnectionConfig $config -Credentials $creds
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [hashtable] $ConnectionConfig,

        [Parameter(Mandatory = $false)]
        [hashtable] $Credentials
    )

    BEGIN {}

    PROCESS {
        $provider = $ConnectionConfig.Provider
        $server = $ConnectionConfig.Server
        $database = $ConnectionConfig.Database
        $authType = $ConnectionConfig.AuthType
        $userId = if ($Credentials) { $Credentials.UserId } else { $ConnectionConfig.UserId }
        $password = if ($Credentials) { $Credentials.Password } else { $ConnectionConfig.Password }

        $connectionString = ""

        switch ($provider) {
            "SqlServer" {
                switch ($authType) {
                    "SqlAuth" {
                        $connectionString = "Server=$server;Database=$database;User Id=$userId;Password=$password;TrustServerCertificate=True;"
                    }
                    "WindowsAuth" {
                        $connectionString = "Server=$server;Database=$database;Integrated Security=True;TrustServerCertificate=True;"
                    }
                }
            }

            "AzureSql" {
                switch ($authType) {
                    "SqlAuth" {
                        $connectionString = "Server=$server;Database=$database;User Id=$userId;Password=$password;Encrypt=True;TrustServerCertificate=False;"
                    }
                    "Interactive" {
                        $connectionString = "Server=$server;Database=$database;Authentication=Active Directory Interactive;Encrypt=True;TrustServerCertificate=False;"
                    }
                    "AzureCli" {
                        # For AzureCli, we'll get a token and use it directly
                        $connectionString = "Server=$server;Database=$database;Encrypt=True;TrustServerCertificate=False;"
                    }
                }
            }

            "Oracle" {
                $connectionString = "Data Source=$server;User Id=$userId;Password=$password;"
            }

            "MySql" {
                $connectionString = "Server=$server;Database=$database;User Id=$userId;Password=$password;"
            }

            "PostgreSql" {
                $connectionString = "Host=$server;Database=$database;Username=$userId;Password=$password;"
            }
        }

        Write-Log -Message "Connection string built for $provider ($authType)" -Level Debug

        return $connectionString
    }

    END {}
}


Function Get-AzureCliToken {
    <#
    .SYNOPSIS
        Gets an Azure AD access token using Azure CLI.
     
    .NOTES
        Name: Get-AzureCliToken
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $token = Get-AzureCliToken
    #>
    
    [CmdletBinding()]
    param()

    BEGIN {}

    PROCESS {
        Write-Log -Message "Retrieving Azure AD token from Azure CLI..." -Level Verbose

        # Check if Azure CLI is installed
        $azCmd = Get-Command "az" -ErrorAction SilentlyContinue
        if (-not $azCmd) {
            throw "Azure CLI is not installed or not in PATH. Please install Azure CLI and run 'az login'."
        }

        # Get access token for Azure SQL
        $tokenJson = az account get-access-token --resource "https://database.windows.net/" --output json 2>&1

        if ($LASTEXITCODE -ne 0) {
            throw "Failed to get Azure access token. Ensure you are logged in with 'az login'. Error: $tokenJson"
        }

        $tokenData = $tokenJson | ConvertFrom-Json
        $accessToken = $tokenData.accessToken

        Write-Log -Message "Azure AD token retrieved successfully" -Level Debug

        return $accessToken
    }

    END {}
}


Function Connect-Database {
    <#
    .SYNOPSIS
        Establishes a database connection for the specified provider.
     
    .NOTES
        Name: Connect-Database
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        $connection = Connect-Database -ConnectionConfig $config -ConnectionName "Source" -TimeoutSeconds 300
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [hashtable] $ConnectionConfig,

        [Parameter(Mandatory = $true, Position = 1)]
        [ValidateSet("Source", "Target")]
        [string] $ConnectionName,

        [Parameter(Mandatory = $false)]
        [int] $TimeoutSeconds = 30
    )

    BEGIN {}

    PROCESS {
        $provider = $ConnectionConfig.Provider

        Write-Log -Message "Connecting to $ConnectionName database ($provider - $($ConnectionConfig.Server))..." -Level Info

        # Install provider if needed
        Install-DatabaseProvider -Provider $provider

        # Get credentials if needed
        $credentials = Get-PromptedCredentials -ConnectionConfig $ConnectionConfig -ConnectionName $ConnectionName

        # Build connection string
        $connectionString = Build-ConnectionString -ConnectionConfig $ConnectionConfig -Credentials $credentials

        # Create connection based on provider
        $connection = $null

        switch ($provider) {
            { $_ -in "SqlServer", "AzureSql" } {
                $connection = New-Object Microsoft.Data.SqlClient.SqlConnection($connectionString)
                $connection.ConnectionTimeout = $TimeoutSeconds

                # For AzureCli auth, set the access token
                if ($ConnectionConfig.AuthType -eq "AzureCli") {
                    $token = Get-AzureCliToken
                    $connection.AccessToken = $token
                }
            }

            "Oracle" {
                $connection = New-Object Oracle.ManagedDataAccess.Client.OracleConnection($connectionString)
            }

            "MySql" {
                $connection = New-Object MySql.Data.MySqlClient.MySqlConnection($connectionString)
            }

            "PostgreSql" {
                $connection = New-Object Npgsql.NpgsqlConnection($connectionString)
            }
        }

        # Open connection
        try {
            $connection.Open()
            Write-Log -Message "$ConnectionName database connection established successfully" -Level Info
        } catch {
            Write-Log -Message "Failed to connect to $ConnectionName database" -Level Error -Exception $_.Exception
            throw
        }

        return $connection
    }

    END {}
}


Function Disconnect-Database {
    <#
    .SYNOPSIS
        Closes a database connection.
     
    .NOTES
        Name: Disconnect-Database
        Author: The Script Ranger
        Version: 1.0
        DateCreated: 2025.06.03
     
    .EXAMPLE
        Disconnect-Database -Connection $connection -ConnectionName "Source"
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [object] $Connection,

        [Parameter(Mandatory = $false)]
        [string] $ConnectionName = "Database"
    )

    BEGIN {}

    PROCESS {
        if ($Connection -and $Connection.State -eq "Open") {
            $Connection.Close()
            $Connection.Dispose()
            Write-Log -Message "$ConnectionName connection closed" -Level Verbose
        }
    }

    END {}
}