/**
 * UberDooberDBMigrator Web Dashboard Server
 * 
 * Express server that serves the web dashboard and provides API endpoints
 * for reading migration progress, errors, and validation data.
 */

const express = require('express');
const path = require('path');
const fs = require('fs');
const chokidar = require('chokidar');

const app = express();
const PORT = process.env.PORT || 3000;

// Paths relative to the Web folder
const ROOT_PATH = path.join(__dirname, '..');
const OUTPUT_PATH = path.join(ROOT_PATH, '_output');
const VALIDATION_PATH = path.join(ROOT_PATH, '_validationoutput');
const MIGRATION_PATH = path.join(ROOT_PATH, '_migration');

// Store connected SSE clients for real-time updates
let clients = [];

// Serve static files from public folder
app.use(express.static(path.join(__dirname, 'public')));

// JSON parsing
app.use(express.json());

/**
 * Helper: Read and parse a JSON file
 */
function readJsonFile(filePath) {
    try {
        if (fs.existsSync(filePath)) {
            const content = fs.readFileSync(filePath, 'utf8');
            return JSON.parse(content);
        }
        return null;
    } catch (error) {
        console.error(`Error reading ${filePath}:`, error.message);
        return null;
    }
}

/**
 * Helper: Get all JSON files in a directory matching a pattern
 */
function getJsonFiles(dirPath, pattern = '') {
    try {
        if (!fs.existsSync(dirPath)) {
            return [];
        }
        
        const files = fs.readdirSync(dirPath)
            .filter(file => file.endsWith('.json'))
            .filter(file => !pattern || file.includes(pattern))
            .map(file => ({
                name: file,
                path: path.join(dirPath, file),
                stats: fs.statSync(path.join(dirPath, file))
            }))
            .sort((a, b) => b.stats.mtime - a.stats.mtime);
        
        return files;
    } catch (error) {
        console.error(`Error reading directory ${dirPath}:`, error.message);
        return [];
    }
}

/**
 * Helper: Get the most recent file matching a pattern
 */
function getMostRecentFile(dirPath, pattern) {
    const files = getJsonFiles(dirPath, pattern);
    return files.length > 0 ? files[0] : null;
}

/**
 * API: Get list of all migrations (based on Progress files)
 */
app.get('/api/migrations', (req, res) => {
    const progressFiles = getJsonFiles(OUTPUT_PATH, 'Progress.json');
    
    const migrations = progressFiles.map(file => {
        const data = readJsonFile(file.path);
        if (!data) return null;
        
        // Extract run ID from filename
        const match = file.name.match(/_(\d{8}_\d{6})_Progress/);
        const runId = match ? match[1] : 'unknown';
        
        return {
            fileName: file.name,
            migrationName: data.migrationName,
            runId: runId,
            status: data.status,
            startedAt: data.startedAt,
            lastUpdatedAt: data.lastUpdatedAt,
            tableCount: data.tables ? data.tables.length : 0,
            modifiedTime: file.stats.mtime
        };
    }).filter(m => m !== null);
    
    res.json(migrations);
});

/**
 * API: Get current/latest migration progress
 */
app.get('/api/progress', (req, res) => {
    const file = getMostRecentFile(OUTPUT_PATH, 'Progress.json');
    
    if (!file) {
        return res.json({ error: 'No progress file found', data: null });
    }
    
    const data = readJsonFile(file.path);
    res.json({ fileName: file.name, data: data });
});

/**
 * API: Get specific migration progress by run ID
 */
app.get('/api/progress/:runId', (req, res) => {
    const { runId } = req.params;
    const files = getJsonFiles(OUTPUT_PATH, `_${runId}_Progress.json`);
    
    if (files.length === 0) {
        return res.status(404).json({ error: 'Progress file not found' });
    }
    
    const data = readJsonFile(files[0].path);
    res.json({ fileName: files[0].name, data: data });
});

/**
 * API: Get current/latest row errors
 */
app.get('/api/row-errors', (req, res) => {
    const file = getMostRecentFile(OUTPUT_PATH, 'RowErrors.json');
    
    if (!file) {
        return res.json({ error: 'No row errors file found', data: null });
    }
    
    const data = readJsonFile(file.path);
    res.json({ fileName: file.name, data: data });
});

/**
 * API: Get row errors by run ID
 */
app.get('/api/row-errors/:runId', (req, res) => {
    const { runId } = req.params;
    const files = getJsonFiles(OUTPUT_PATH, `_${runId}_RowErrors.json`);
    
    if (files.length === 0) {
        return res.status(404).json({ error: 'Row errors file not found' });
    }
    
    const data = readJsonFile(files[0].path);
    res.json({ fileName: files[0].name, data: data });
});

/**
 * API: Get current/latest error log
 */
app.get('/api/error-log', (req, res) => {
    const file = getMostRecentFile(OUTPUT_PATH, 'ErrorLog.json');
    
    if (!file) {
        return res.json({ error: 'No error log file found', data: null });
    }
    
    const data = readJsonFile(file.path);
    res.json({ fileName: file.name, data: data });
});

/**
 * API: Get error log by run ID
 */
app.get('/api/error-log/:runId', (req, res) => {
    const { runId } = req.params;
    const files = getJsonFiles(OUTPUT_PATH, `_${runId}_ErrorLog.json`);
    
    if (files.length === 0) {
        return res.status(404).json({ error: 'Error log file not found' });
    }
    
    const data = readJsonFile(files[0].path);
    res.json({ fileName: files[0].name, data: data });
});

/**
 * API: Get list of all validations
 */
app.get('/api/validations', (req, res) => {
    const validationFiles = getJsonFiles(VALIDATION_PATH, 'Validation.json');
    
    const validations = validationFiles.map(file => {
        const data = readJsonFile(file.path);
        if (!data) return null;
        
        // Extract run ID from filename
        const match = file.name.match(/_(\d{8}_\d{6})_Validation/);
        const runId = match ? match[1] : 'unknown';
        
        return {
            fileName: file.name,
            migrationName: data.migrationName,
            runId: runId,
            isValid: data.isValid,
            validatedAt: data.validatedAt,
            summary: data.summary,
            modifiedTime: file.stats.mtime
        };
    }).filter(v => v !== null);
    
    res.json(validations);
});

/**
 * API: Get current/latest validation
 */
app.get('/api/validation', (req, res) => {
    const file = getMostRecentFile(VALIDATION_PATH, 'Validation.json');
    
    if (!file) {
        return res.json({ error: 'No validation file found', data: null });
    }
    
    const data = readJsonFile(file.path);
    res.json({ fileName: file.name, data: data });
});

/**
 * API: Get validation by run ID
 */
app.get('/api/validation/:runId', (req, res) => {
    const { runId } = req.params;
    const files = getJsonFiles(VALIDATION_PATH, `_${runId}_Validation.json`);
    
    if (files.length === 0) {
        return res.status(404).json({ error: 'Validation file not found' });
    }
    
    const data = readJsonFile(files[0].path);
    res.json({ fileName: files[0].name, data: data });
});

/**
 * API: Get master configuration
 */
app.get('/api/config', (req, res) => {
    const configPath = path.join(MIGRATION_PATH, 'MasterConfig.xml');
    
    try {
        if (fs.existsSync(configPath)) {
            const content = fs.readFileSync(configPath, 'utf8');
            res.json({ exists: true, content: content });
        } else {
            res.json({ exists: false, content: null });
        }
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

/**
 * API: Get dashboard summary (aggregated data)
 */
app.get('/api/dashboard', (req, res) => {
    // Get latest progress
    const progressFile = getMostRecentFile(OUTPUT_PATH, 'Progress.json');
    const progress = progressFile ? readJsonFile(progressFile.path) : null;
    
    // Get latest row errors
    const rowErrorsFile = getMostRecentFile(OUTPUT_PATH, 'RowErrors.json');
    const rowErrors = rowErrorsFile ? readJsonFile(rowErrorsFile.path) : null;
    
    // Get latest error log
    const errorLogFile = getMostRecentFile(OUTPUT_PATH, 'ErrorLog.json');
    const errorLog = errorLogFile ? readJsonFile(errorLogFile.path) : null;
    
    // Get latest validation
    const validationFile = getMostRecentFile(VALIDATION_PATH, 'Validation.json');
    const validation = validationFile ? readJsonFile(validationFile.path) : null;
    
    // Calculate summary statistics
    let summary = {
        hasMigration: progress !== null,
        migrationName: progress?.migrationName || 'N/A',
        status: progress?.status || 'No Migration',
        startedAt: progress?.startedAt || null,
        lastUpdatedAt: progress?.lastUpdatedAt || null,
        tables: {
            total: 0,
            completed: 0,
            inProgress: 0,
            pending: 0,
            failed: 0
        },
        rows: {
            total: 0,
            processed: 0,
            percentage: 0
        },
        errors: {
            rowErrors: rowErrors?.totalRowErrors || 0,
            logEntries: errorLog?.totalEntries || 0
        },
        validation: validation ? {
            isValid: validation.isValid,
            validatedAt: validation.validatedAt,
            errorsFound: validation.summary?.errorsFound || 0,
            warningsFound: validation.summary?.warningsFound || 0
        } : null
    };
    
    if (progress && progress.tables) {
        summary.tables.total = progress.tables.length;
        
        progress.tables.forEach(table => {
            summary.rows.total += table.totalRows || 0;
            summary.rows.processed += table.processedRows || 0;
            
            switch (table.status) {
                case 'Completed':
                    summary.tables.completed++;
                    break;
                case 'InProgress':
                    summary.tables.inProgress++;
                    break;
                case 'Failed':
                    summary.tables.failed++;
                    break;
                default:
                    summary.tables.pending++;
            }
        });
        
        if (summary.rows.total > 0) {
            summary.rows.percentage = Math.round((summary.rows.processed / summary.rows.total) * 100);
        }
    }
    
    res.json(summary);
});

/**
 * Server-Sent Events endpoint for real-time updates
 */
app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    
    // Send initial connection message
    res.write('data: {"type": "connected"}\n\n');
    
    // Add client to list
    clients.push(res);
    
    // Remove client on disconnect
    req.on('close', () => {
        clients = clients.filter(client => client !== res);
    });
});

/**
 * Broadcast update to all connected SSE clients
 */
function broadcastUpdate(type, data) {
    const message = JSON.stringify({ type, data, timestamp: new Date().toISOString() });
    clients.forEach(client => {
        client.write(`data: ${message}\n\n`);
    });
}

/**
 * Watch for file changes and broadcast updates
 */
function setupFileWatcher() {
    const watchPaths = [OUTPUT_PATH, VALIDATION_PATH];
    
    // Create directories if they don't exist
    watchPaths.forEach(p => {
        if (!fs.existsSync(p)) {
            fs.mkdirSync(p, { recursive: true });
        }
    });
    
    const watcher = chokidar.watch(watchPaths, {
        persistent: true,
        ignoreInitial: true,
        awaitWriteFinish: {
            stabilityThreshold: 500,
            pollInterval: 100
        }
    });
    
    watcher.on('change', (filePath) => {
        console.log(`File changed: ${filePath}`);
        
        if (filePath.includes('Progress.json')) {
            broadcastUpdate('progress', { file: path.basename(filePath) });
        } else if (filePath.includes('RowErrors.json')) {
            broadcastUpdate('rowErrors', { file: path.basename(filePath) });
        } else if (filePath.includes('ErrorLog.json')) {
            broadcastUpdate('errorLog', { file: path.basename(filePath) });
        } else if (filePath.includes('Validation.json')) {
            broadcastUpdate('validation', { file: path.basename(filePath) });
        }
    });
    
    watcher.on('add', (filePath) => {
        console.log(`File added: ${filePath}`);
        broadcastUpdate('fileAdded', { file: path.basename(filePath) });
    });
    
    console.log('File watcher initialized');
}

// Catch-all route to serve index.html for SPA-style navigation
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, () => {
    console.log('========================================');
    console.log('UberDooberDBMigrator Web Dashboard');
    console.log('========================================');
    console.log(`Server running at: http://localhost:${PORT}`);
    console.log(`Monitoring: ${OUTPUT_PATH}`);
    console.log(`Validation: ${VALIDATION_PATH}`);
    console.log('========================================');
    
    // Setup file watcher for real-time updates
    setupFileWatcher();
});
