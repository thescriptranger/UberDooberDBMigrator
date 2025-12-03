# UberDooberDBMigrator Web Dashboard

A real-time web dashboard for monitoring UberDooberDBMigrator migration progress, errors, and validation results.

## Features

- **Real-time Updates**: Automatically refreshes when migration files change
- **Dashboard View**: Overall migration status, progress bars, and statistics
- **Tables View**: Detailed progress for each table being migrated
- **Error Log View**: All error log entries with timestamps
- **Row Errors View**: Failed rows with full source data for reprocessing
- **Validation View**: Validation results with sample transformed data
- **History View**: Browse past migrations and validations
- **Export**: Export row errors for external processing
- **Dark Theme**: Easy on the eyes during long migration monitoring sessions

## Prerequisites

- **Node.js 16+** (LTS recommended)
- **npm** (comes with Node.js)

To check if Node.js is installed:
```bash
node --version
npm --version
```

If not installed, download from: https://nodejs.org/

## Installation

1. Navigate to the Web folder:
   ```bash
   cd Web
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

## Running the Dashboard

Start the web server:
```bash
npm start
```

The dashboard will be available at: **http://localhost:3000**

To use a different port:
```bash
PORT=8080 npm start
```

## How It Works

1. The Express server monitors the `_output` and `_validationoutput` folders for JSON files
2. When files change, connected browsers receive real-time updates via Server-Sent Events (SSE)
3. The dashboard displays data from the most recent migration/validation run
4. Auto-refresh polls for updates every 3 seconds (can be disabled via toggle)

## Dashboard Views

### Dashboard
- Migration status (Pending, In Progress, Completed, Failed)
- Tables completed vs. total
- Rows processed percentage with progress bar
- Total errors and row errors count
- Quick view of table progress
- Recent errors list
- Last validation summary

### Tables
- Detailed table-by-table progress
- Filter by status (All, Completed, In Progress, Pending, Failed)
- Source and target table names
- Progress percentage and row counts
- Last processed key value

### Error Log
- All error entries from ErrorLog JSON
- Timestamp, table, and message for each entry
- Reverse chronological order

### Row Errors
- Expandable sections per table
- View source key, timestamp, and error message
- Click to view full source data in modal
- Export to JSON for reprocessing

### Validation
- Overall validation status (Valid/Invalid)
- Error and warning counts
- Connection test results
- Per-table validation with:
  - Errors and warnings
  - Sample data showing source vs. transformed output

### History
- List of all migration runs
- List of all validation runs
- Click to view specific run details

## API Endpoints

The server provides the following REST API endpoints:

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
| `GET /api/events` | Server-Sent Events stream |

## File Structure

```
Web/
├── package.json          # Node.js project configuration
├── server.js             # Express server with API endpoints
├── README.md             # This file
└── public/               # Static web files
    ├── index.html        # Main HTML page
    ├── css/
    │   └── styles.css    # Custom styles
    └── js/
        └── app.js        # Application JavaScript
```

## Configuration

The server automatically detects paths relative to its location:

- **Output Path**: `../_output` (migration JSON files)
- **Validation Path**: `../_validationoutput` (validation JSON files)
- **Migration Path**: `../_migration` (configuration files)

## Browser Support

Tested with:
- Chrome 90+
- Firefox 88+
- Edge 90+
- Safari 14+

## Troubleshooting

### "Cannot find module 'express'"
Run `npm install` in the Web folder.

### Dashboard shows "No migration data"
Ensure migration JSON files exist in the `_output` folder. The example files (prefixed with `EXAMPLE_`) can be used for testing.

### Real-time updates not working
Check browser console for SSE connection errors. The connection status indicator in the top-right shows current state.

### Port already in use
Use a different port: `PORT=8080 npm start`

## Development

To modify the dashboard:

1. Edit files in `public/` folder
2. Changes to HTML/CSS/JS take effect on browser refresh
3. Changes to `server.js` require server restart

## Dependencies

- **express** (^4.18.2): Web server framework
- **chokidar** (^3.5.3): File system watcher for real-time updates

## License

Same license as UberDooberDBMigrator.
