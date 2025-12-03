# UberDooberDBMigrator

A PowerShell-based database migration tool that maps and transforms data from various source databases to SQL Server or Azure SQL targets using XML configuration files.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Folder Structure](#folder-structure)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Global Configuration](#global-configuration)
  - [Master Configuration](#master-configuration)
  - [Table Mapping Configuration](#table-mapping-configuration)
- [Transformation Types](#transformation-types)
- [Usage](#usage)
  - [Parameters](#parameters)
  - [Running a Migration](#running-a-migration)
  - [Validation Mode](#validation-mode)
  - [Resuming a Failed Migration](#resuming-a-failed-migration)
  - [Table Filtering](#table-filtering)
- [Output Files](#output-files)
- [Authentication](#authentication)
- [Error Handling](#error-handling)
- [Web Dashboard](#web-dashboard)
  - [Dashboard Prerequisites](#dashboard-prerequisites)
  - [Starting the Dashboard](#starting-the-dashboard)
  - [Dashboard Views](#dashboard-views)
  - [Real-time Updates](#real-time-updates)
  - [API Endpoints](#api-endpoints)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

---

## Overview

UberDooberDBMigrator is designed to migrate data between databases with different schemas. It supports complex transformations including column concatenation, splitting, lookups, calculations, and foreign key remapping when identity columns generate new values.

The tool uses XML configuration files to define source-to-target mappings, making migrations repeatable and version-controllable.

---

## Features

- **Multi-Database Source Support**: Oracle, SQL Server, Azure SQL, MySQL, PostgreSQL
- **Target Support**: SQL Server and Azure SQL
- **Data Transformations**: Simple mapping, concatenation, split, lookup, calculated, static, conditional, convert, and key lookup
- **Batch Processing**: Configurable batch sizes for large datasets
- **Resumable Migrations**: Resume from where a failed migration left off
- **Validation Mode**: Validate configuration without executing migration
- **Identity Handling**: Preserve original keys or generate new identities with automatic foreign key remapping
- **Progress Tracking**: JSON-based progress files for monitoring and resume capability
- **Error Logging**: Separate row-level error tracking for reprocessing failed rows
- **Azure AD Authentication**: Support for Interactive browser login and Azure CLI token authentication
- **Web Dashboard**: Real-time monitoring interface for migration progress and errors

---

## Prerequisites

- **PowerShell 5.1 or later** (PowerShell 7+ recommended)
- **Network access** to source and target databases
- **Appropriate database permissions** on both source (SELECT) and target (INSERT, ALTER for constraints)
- **Azure CLI** (if using AzureCli authentication for Azure SQL)

The script will automatically install required database provider packages (NuGet) on first run:
- Microsoft.Data.SqlClient (SQL Server/Azure SQL)
- Oracle.ManagedDataAccess.Core (Oracle)
- MySql.Data (MySQL)
- Npgsql (PostgreSQL)

---

## Folder Structure

```
UberDooberDBMigrator/
├── UberDooberDBMigrator.ps1          # Main script
├── UberDooberDBMigrator.xml          # Global script configuration
├── README.md                          # This file
├── _function/                         # PowerShell function modules
│   ├── Connect-Database.ps1
│   ├── Invoke-BatchInsert.ps1
│   ├── Invoke-ColumnTransform.ps1
│   ├── Invoke-KeyMapOperation.ps1
│   ├── Invoke-TableMigration.ps1
│   ├── Read-Configuration.ps1
│   ├── Set-DatabaseConstraints.ps1
│   ├── Test-MigrationConfiguration.ps1
│   ├── Write-Log.ps1
│   └── Write-MigrationOutput.ps1
├── _migration/                        # Migration configuration
│   ├── MasterConfig.xml              # Master migration configuration
│   └── _tablemappings/               # Table-specific mapping files
│       ├── SourceDB.Customers.xml
│       └── SourceDB.Orders.xml
├── _output/                           # Runtime output (auto-created)
│   ├── *_Progress.json
│   ├── *_RowErrors.json
│   └── *_ErrorLog.json
├── _validationoutput/                 # Validation output (auto-created)
│   └── *_Validation.json
├── _logs/                             # PowerShell transcripts (auto-created)
│   └── *.log
└── Web/                               # Web dashboard
    ├── package.json                   # Node.js dependencies
    ├── server.js                      # Express server
    ├── README.md                      # Web dashboard documentation
    └── public/                        # Static web files
        ├── index.html
        ├── css/styles.css
        └── js/app.js
```

---

## Installation

1. Download or clone the UberDooberDBMigrator folder to your desired location.

2. Ensure PowerShell execution policy allows running scripts:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

3. Configure your migration by editing the XML files (see [Configuration](#configuration)).

4. Run the script:
   ```powershell
   .\UberDooberDBMigrator.ps1
   ```

---

## Configuration

### Global Configuration

**File**: `UberDooberDBMigrator.xml`

This file contains global variables and default settings for the script.

```xml
<?xml version="1.0"?>
<configuration>
    <variables>
        <!-- Environment identifier (DEV, QA, UAT, PROD) -->
        <Environment>DEV</Environment>
        
        <!-- Default log level if not specified via parameter -->
        <DefaultLogLevel>Info</DefaultLogLevel>
    </variables>
</configuration>
```

| Setting | Description |
|---------|-------------|
| `Environment` | Environment identifier for logging purposes |
| `DefaultLogLevel` | Default log level: Error, Warning, Info, Verbose, Debug |

---

### Master Configuration

**File**: `_migration/MasterConfig.xml`

This file defines the migration settings, database connections, and table mappings.

```xml
<?xml version="1.0"?>
<MigrationConfig>
    <MigrationName>MyMigration</MigrationName>
    <BatchSize>10000</BatchSize>
    <QueryTimeoutSeconds>300</QueryTimeoutSeconds>

    <SourceConnection>
        <Provider>SqlServer</Provider>
        <Server>source-server.company.com</Server>
        <Database>SourceDB</Database>
        <AuthType>SqlAuth</AuthType>
        <UserId>username</UserId>
        <Password></Password>
    </SourceConnection>

    <TargetConnection>
        <Provider>AzureSql</Provider>
        <Server>target-server.database.windows.net</Server>
        <Database>TargetDB</Database>
        <AuthType>AzureCli</AuthType>
        <UserId></UserId>
        <Password></Password>
    </TargetConnection>

    <Tables>
        <Table order="1" tableFilter="true">
            <File>SourceDB.Customers.xml</File>
            <Source schema="dbo" table="Customers" />
            <Target schema="dbo" table="tblCustomers" />
            <BatchColumn>CustID</BatchColumn>
            <ColumnMappings>
                <Column source="CustID" target="CustomerID" />
                <Column source="Email" target="EmailAddress" />
                <Column source="CreatedDate" target="CreatedDate" sourceFormat="yyyy-MM-dd" />
            </ColumnMappings>
        </Table>
    </Tables>
</MigrationConfig>
```

#### Migration Settings

| Setting | Description |
|---------|-------------|
| `MigrationName` | Unique name for this migration (used in output file names) |
| `BatchSize` | Number of rows per batch. Set to `0` to disable batching |
| `QueryTimeoutSeconds` | Query timeout in seconds |

#### Connection Settings

| Setting | Description |
|---------|-------------|
| `Provider` | Database provider: `SqlServer`, `AzureSql`, `Oracle`, `MySql`, `PostgreSql` |
| `Server` | Server hostname or IP address |
| `Database` | Database name |
| `AuthType` | Authentication type (see [Authentication](#authentication)) |
| `UserId` | Username (leave empty to prompt at runtime) |
| `Password` | Password (leave empty to prompt at runtime) |

#### Table Settings

| Attribute/Element | Description |
|-------------------|-------------|
| `order` | Execution order (required) |
| `tableFilter` | Set to `true` to include when using `-TableFilter` parameter |
| `File` | Reference to table mapping XML file (optional) |
| `Source schema/table` | Source table identification |
| `Target schema/table` | Target table identification |
| `BatchColumn` | Column used for batching and resumability |
| `ColumnMappings` | Simple column-to-column mappings |
| `sourceFormat` | Optional date format for parsing source dates |

---

### Table Mapping Configuration

**Location**: `_migration/_tablemappings/<DatabaseName>.<TableName>.xml`

Table mapping files are only needed when a table requires:
- Column transformations (beyond simple mapping)
- Table-level settings (identity handling, existing data action)

```xml
<?xml version="1.0"?>
<TableMapping>
    <Source schema="dbo" table="Customers" database="SourceDB" />
    <Target schema="dbo" table="tblCustomers" />

    <Settings>
        <IdentityHandling>GenerateNew</IdentityHandling>
        <IdentityColumn>CustomerID</IdentityColumn>
        <BatchColumn>CustID</BatchColumn>
        <ExistingDataAction>Truncate</ExistingDataAction>
    </Settings>

    <Columns>
        <!-- Transformation definitions -->
    </Columns>
</TableMapping>
```

#### Table Settings

| Setting | Values | Description |
|---------|--------|-------------|
| `IdentityHandling` | `PreserveKeys`, `GenerateNew` | How to handle identity columns |
| `IdentityColumn` | Column name | Target identity column (required if `GenerateNew`) |
| `BatchColumn` | Column name | Source column for batching |
| `ExistingDataAction` | `Truncate`, `Append` | Action for existing target data |

---

## Transformation Types

### Simple Mapping

Direct column-to-column mapping. Typically defined in MasterConfig, but can be in table mapping.

```xml
<Column type="simple">
    <Source column="FirstName" />
    <Target column="GivenName" nullDefault="Unknown" />
</Column>
```

### Concatenation

Combine multiple source columns and/or literals into one target column.

```xml
<Column type="concat">
    <Source>
        <Part column="FirstName" />
        <Part literal=" " />
        <Part column="LastName" />
    </Source>
    <Target column="FullName" />
</Column>
```

### Split

Split one source column into multiple target columns by a delimiter.

```xml
<Column type="split">
    <Source column="FullAddress" />
    <Delimiter value=", " />
    <Targets>
        <Part index="0" column="Street" />
        <Part index="1" column="City" />
        <Part index="2" column="State" />
    </Targets>
</Column>
```

### Lookup

Map source values to target values using an inline lookup table.

```xml
<Column type="lookup">
    <Source column="StatusCode" />
    <Target column="StatusID" nullDefault="0" />
    <LookupTable>
        <Map from="A" to="1" />
        <Map from="I" to="2" />
        <Map from="P" to="3" />
        <Default to="0" />
    </LookupTable>
</Column>
```

### Calculated

Evaluate an expression using source column values.

```xml
<Column type="calculated">
    <Expression>Price * Quantity</Expression>
    <Target column="TotalAmount" />
</Column>
```

### Static

Insert a hardcoded value or SQL function result.

```xml
<!-- Literal value -->
<Column type="static">
    <Target column="Source" />
    <Value literal="LegacySystem" />
</Column>

<!-- SQL function -->
<Column type="static">
    <Target column="MigratedDate" />
    <Value function="GETDATE()" />
</Column>
```

Supported functions: `GETDATE()`, `GETUTCDATE()`, `NEWID()`, `SUSER_SNAME()`

### Conditional

Apply different mappings based on source data values.

```xml
<Column type="conditional">
    <Target column="CustomerType" />
    <When test="AccountType = 'B'">
        <Value literal="Business" />
    </When>
    <When test="AccountType = 'P'">
        <Value literal="Personal" />
    </When>
    <Else>
        <Value literal="Unknown" />
    </Else>
</Column>
```

Supported operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `IS NULL`, `IS NOT NULL`

### Convert

Explicit data type conversion with format specification.

```xml
<Column type="convert">
    <Source column="BirthDateString" format="MM/dd/yyyy" />
    <Target column="BirthDate" type="datetime" />
</Column>
```

Supported target types: `datetime`, `datetime2`, `date`, `smalldatetime`, `int`, `bigint`, `decimal`, `float`, `bit`, `varchar`, `nvarchar`, `uniqueidentifier`

### Key Lookup

Reference new identity values from a previously migrated table. Used for foreign key remapping when parent tables generate new identities.

```xml
<Column type="keyLookup">
    <Source column="CustID" />
    <Target column="CustomerID" nullDefault="-1" />
    <KeyMap sourceTable="SourceDB.Customers" sourceKeyColumn="CustID" />
</Column>
```

**Important**: The referenced table must:
1. Be migrated before this table (lower `order` value)
2. Have `IdentityHandling` set to `GenerateNew`

---

## Usage

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `-ValidateOnly` | Switch | Validate configuration without migrating data |
| `-Resume` | Switch | Continue from where a previous run left off |
| `-TableFilter` | Switch | Only process tables with `tableFilter="true"` |
| `-LogLevel` | String | Log level: `Error`, `Warning`, `Info`, `Verbose`, `Debug` |

### Running a Migration

Basic migration with default settings:

```powershell
.\UberDooberDBMigrator.ps1
```

With verbose logging:

```powershell
.\UberDooberDBMigrator.ps1 -LogLevel Verbose
```

### Validation Mode

Validate your configuration before running the actual migration:

```powershell
.\UberDooberDBMigrator.ps1 -ValidateOnly
```

Validation checks:
- XML structure and required fields
- Database connectivity
- Source and target table existence
- Column existence and mapping validity
- Transformation configuration
- Sample data transformation preview

Output is written to `_validationoutput/`.

### Resuming a Failed Migration

If a migration fails mid-way, resume from where it left off:

```powershell
.\UberDooberDBMigrator.ps1 -Resume
```

The script will:
1. Find the most recent progress file
2. Skip completed tables
3. Resume the in-progress table from the last successful batch

**Note**: If no progress file exists, the script will fail with an error.

### Table Filtering

Run only specific tables by marking them in MasterConfig:

```xml
<Table order="1" tableFilter="true">
    <!-- This table will run -->
</Table>
<Table order="2">
    <!-- This table will be skipped -->
</Table>
```

Then run with the `-TableFilter` parameter:

```powershell
.\UberDooberDBMigrator.ps1 -TableFilter
```

---

## Output Files

All output files follow the naming convention:
`UberDooberDBMigrator_<MigrationName>_<Timestamp>_<Type>.json`

### Progress File

**Location**: `_output/`

Tracks migration progress for each table. Used for resumability.

```json
{
    "migrationName": "MyMigration",
    "startedAt": "2025-06-03T14:30:00Z",
    "lastUpdatedAt": "2025-06-03T14:45:00Z",
    "status": "InProgress",
    "tables": [
        {
            "sourceTable": "dbo.Customers",
            "targetTable": "dbo.tblCustomers",
            "status": "Completed",
            "totalRows": 50000,
            "processedRows": 50000,
            "lastBatchKeyValue": null
        }
    ]
}
```

### Row Errors File

**Location**: `_output/`

Captures failed rows with enough detail to reprocess them later.

```json
{
    "migrationName": "MyMigration",
    "migrationRunId": "20250603_143000",
    "generatedAt": "2025-06-03T14:45:00Z",
    "totalRowErrors": 5,
    "tables": [
        {
            "sourceTable": "dbo.Orders",
            "targetTable": "sales.CustomerOrders",
            "errorCount": 5,
            "rows": [
                {
                    "sourceKeyValue": "10452",
                    "errorTimestamp": "2025-06-03T14:42:15Z",
                    "errorMessage": "String or binary data would be truncated",
                    "sourceData": {
                        "OrderID": 10452,
                        "Notes": "Very long string..."
                    }
                }
            ]
        }
    ]
}
```

### Error Log File

**Location**: `_output/`

General error log (errors only, not informational messages).

```json
{
    "migrationName": "MyMigration",
    "migrationRunId": "20250603_143000",
    "generatedAt": "2025-06-03T14:45:00Z",
    "totalEntries": 2,
    "entries": [
        {
            "timestamp": "2025-06-03T14:42:15Z",
            "level": "Error",
            "table": "dbo.Orders",
            "message": "Row insert failed for key 10452: String or binary data would be truncated"
        }
    ]
}
```

### Validation File

**Location**: `_validationoutput/`

Comprehensive validation results including sample transformed data.

```json
{
    "migrationName": "MyMigration",
    "validatedAt": "2025-06-03T14:30:00Z",
    "isValid": true,
    "summary": {
        "tablesValidated": 3,
        "errorsFound": 0,
        "warningsFound": 2
    },
    "configuration": { ... },
    "connections": { ... },
    "tables": [
        {
            "sourceTable": "dbo.Customers",
            "targetTable": "dbo.tblCustomers",
            "isValid": true,
            "sourceRowCount": 50000,
            "errors": [],
            "warnings": ["Target column 'MiddleName' is not mapped"],
            "sampleData": [
                {
                    "source": { "CustID": 1, "FirstName": "John" },
                    "transformed": { "CustomerID": 1, "GivenName": "John" }
                }
            ]
        }
    ]
}
```

### PowerShell Logs

**Location**: `_logs/`

Full PowerShell transcript of each execution.

---

## Authentication

### SqlAuth

Standard username/password authentication. If credentials are not provided in the XML, the script will prompt at runtime.

```xml
<AuthType>SqlAuth</AuthType>
<UserId>myuser</UserId>
<Password></Password>  <!-- Will prompt -->
```

### WindowsAuth

Windows Integrated Authentication (SQL Server on-premises only).

```xml
<AuthType>WindowsAuth</AuthType>
```

### Interactive

Azure AD Interactive browser login. Opens a browser for authentication.

```xml
<AuthType>Interactive</AuthType>
```

### AzureCli

Uses an existing Azure CLI login session. You must run `az login` before executing the script.

```xml
<AuthType>AzureCli</AuthType>
```

To login:
```bash
az login
```

---

## Error Handling

### Row-Level Errors

When a single row fails to insert:
1. The error is logged to the RowErrors JSON file
2. The migration continues with the next row
3. Source data is captured for potential reprocessing

### Batch/Table Errors

When a batch or table-level error occurs:
1. The error is logged to the ErrorLog JSON file
2. Progress is saved
3. The migration fails and can be resumed

### Transient Errors

Network blips, timeouts, and connection errors cause immediate failure. Use `-Resume` to continue after resolving the issue.

### Constraint Handling

- All foreign key and check constraints are disabled before migration
- Constraints are re-enabled after migration completes (even on failure)
- Triggers are disabled per-table during migration

### Key Map Cleanup

Temporary key mapping tables (`_KeyMap_*`) are always cleaned up, even on failure, to prevent resource issues on the target database.

---

## Web Dashboard

UberDooberDBMigrator includes a real-time web dashboard for monitoring migration progress, viewing errors, and reviewing validation results.

### Dashboard Prerequisites

- **Node.js 16+** (LTS recommended)
- **npm** (comes with Node.js)

To check if Node.js is installed:
```bash
node --version
npm --version
```

If not installed, download from: https://nodejs.org/

### Starting the Dashboard

1. Navigate to the Web folder:
   ```bash
   cd Web
   ```

2. Install dependencies (first time only):
   ```bash
   npm install
   ```

3. Start the server:
   ```bash
   npm start
   ```

4. Open your browser to: **http://localhost:3000**

To use a different port:
```bash
PORT=8080 npm start
```

### Dashboard Views

| View | Description |
|------|-------------|
| **Dashboard** | Overall migration status, progress bars, table summary, recent errors, validation summary |
| **Tables** | Detailed per-table progress with filtering by status (All, Completed, In Progress, Pending, Failed) |
| **Error Log** | Chronological list of all error log entries with timestamps and table names |
| **Row Errors** | Expandable sections per table showing failed rows with full source data; includes export functionality |
| **Validation** | Validation results with connection status, per-table errors/warnings, and sample transformed data |
| **History** | Browse past migration and validation runs |

### Real-time Updates

The dashboard automatically updates when migration files change:

1. **Server-Sent Events (SSE)**: File watcher triggers instant browser updates when JSON files in `_output` or `_validationoutput` are modified
2. **Auto-refresh**: Polls the server every 3 seconds (can be toggled off in the navbar)
3. **Connection Status**: The navbar shows a colored indicator for connection status (green = connected, red = disconnected)

### API Endpoints

The web server provides REST API endpoints for programmatic access:

| Endpoint | Description |
|----------|-------------|
| `GET /api/dashboard` | Aggregated dashboard summary |
| `GET /api/migrations` | List of all migration runs |
| `GET /api/progress` | Latest progress data |
| `GET /api/progress/:runId` | Specific migration progress |
| `GET /api/row-errors` | Latest row errors |
| `GET /api/row-errors/:runId` | Specific run row errors |
| `GET /api/error-log` | Latest error log |
| `GET /api/error-log/:runId` | Specific run error log |
| `GET /api/validations` | List of all validations |
| `GET /api/validation` | Latest validation data |
| `GET /api/validation/:runId` | Specific validation run |
| `GET /api/config` | Master configuration XML |
| `GET /api/events` | Server-Sent Events stream for real-time updates |

---

## Examples

### Example 1: Simple Table Migration

MasterConfig.xml:
```xml
<Table order="1">
    <Source schema="dbo" table="Countries" />
    <Target schema="ref" table="Countries" />
    <BatchColumn>CountryCode</BatchColumn>
    <ColumnMappings>
        <Column source="CountryCode" target="CountryCode" />
        <Column source="CountryName" target="Name" />
    </ColumnMappings>
</Table>
```

No table mapping file needed for simple mappings.

### Example 2: Table with Transformations

MasterConfig.xml:
```xml
<Table order="1" tableFilter="true">
    <File>SourceDB.Customers.xml</File>
    <Source schema="dbo" table="Customers" />
    <Target schema="dbo" table="tblCustomers" />
    <BatchColumn>CustID</BatchColumn>
    <ColumnMappings>
        <Column source="CustID" target="CustomerID" />
        <Column source="Email" target="EmailAddress" />
    </ColumnMappings>
</Table>
```

SourceDB.Customers.xml:
```xml
<TableMapping>
    <Source schema="dbo" table="Customers" database="SourceDB" />
    <Target schema="dbo" table="tblCustomers" />

    <Settings>
        <IdentityHandling>GenerateNew</IdentityHandling>
        <IdentityColumn>CustomerID</IdentityColumn>
        <ExistingDataAction>Truncate</ExistingDataAction>
    </Settings>

    <Columns>
        <Column type="concat">
            <Source>
                <Part column="FirstName" />
                <Part literal=" " />
                <Part column="LastName" />
            </Source>
            <Target column="FullName" />
        </Column>

        <Column type="lookup">
            <Source column="Status" />
            <Target column="StatusID" />
            <LookupTable>
                <Map from="A" to="1" />
                <Map from="I" to="2" />
                <Default to="0" />
            </LookupTable>
        </Column>
    </Columns>
</TableMapping>
```

### Example 3: Parent-Child with Key Remapping

Parent table (Customers) generates new identities:
```xml
<Table order="1">
    <File>SourceDB.Customers.xml</File>
    <!-- IdentityHandling: GenerateNew -->
</Table>
```

Child table (Orders) references the new parent IDs:
```xml
<Table order="2">
    <File>SourceDB.Orders.xml</File>
    <!-- Uses keyLookup to map old CustID to new CustomerID -->
</Table>
```

SourceDB.Orders.xml:
```xml
<Columns>
    <Column type="keyLookup">
        <Source column="CustID" />
        <Target column="CustomerID" />
        <KeyMap sourceTable="SourceDB.Customers" sourceKeyColumn="CustID" />
    </Column>
</Columns>
```

---

## Troubleshooting

### PowerShell Script Issues

#### "Resume requested but no progress file found"
You cannot use `-Resume` without a previous run. Run without `-Resume` first.

#### "Master configuration file not found"
Ensure `_migration/MasterConfig.xml` exists and the path is correct.

#### "Table mapping file not found"
The `<File>` reference in MasterConfig doesn't match a file in `_migration/_tablemappings/`.

#### "Failed to connect to database"
Check:
- Server name and network connectivity
- Database provider is correct
- Credentials are valid
- For Azure, ensure you've run `az login` if using AzureCli auth

#### "Source/Target table does not exist"
Verify the schema and table names in your configuration match the actual database objects.

#### "Key map not found for source table"
The `keyLookup` references a table that either:
- Wasn't migrated before this table (check `order` values)
- Doesn't have `IdentityHandling: GenerateNew` set

#### Provider installation failures
If NuGet package installation fails:
1. Run PowerShell as Administrator
2. Ensure internet connectivity
3. Manually install: `Install-Package -Name "Microsoft.Data.SqlClient" -Source "nuget.org"`

#### Memory issues with large tables
Reduce `BatchSize` in MasterConfig to process fewer rows at a time.

### Web Dashboard Issues

#### "Cannot find module 'express'"
Run `npm install` in the Web folder.

#### Dashboard shows "No migration data"
Ensure migration JSON files exist in the `_output` folder. The example files (prefixed with `EXAMPLE_`) can be used for testing.

#### Real-time updates not working
Check browser console for SSE connection errors. The connection status indicator in the top-right shows current state.

#### Port already in use
Use a different port: `PORT=8080 npm start`

---

## License

This is Public - feel free to use as you wish, or create your own branch and push to this repo.

## Support

thescriptranger@gmail.com