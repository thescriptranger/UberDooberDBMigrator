/**
 * UberDooberDBMigrator Web Dashboard
 * Main Application JavaScript
 */

// Global state
let currentView = 'dashboard';
let autoRefresh = true;
let refreshInterval = null;
let eventSource = null;
let dashboardData = null;
let progressData = null;
let rowErrorsData = null;
let errorLogData = null;
let validationData = null;

// Refresh interval in milliseconds
const REFRESH_INTERVAL = 3000;

/**
 * Initialize the application
 */
document.addEventListener('DOMContentLoaded', () => {
    // Initialize auto-refresh toggle
    const autoRefreshToggle = document.getElementById('autoRefreshToggle');
    autoRefreshToggle.addEventListener('change', (e) => {
        autoRefresh = e.target.checked;
        if (autoRefresh) {
            startAutoRefresh();
        } else {
            stopAutoRefresh();
        }
    });

    // Initial data load
    loadAllData();

    // Start auto-refresh
    startAutoRefresh();

    // Connect to Server-Sent Events
    connectSSE();
});

/**
 * API Functions
 */
async function fetchApi(endpoint) {
    try {
        const response = await fetch(`/api/${endpoint}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error fetching ${endpoint}:`, error);
        return null;
    }
}

async function loadDashboard() {
    dashboardData = await fetchApi('dashboard');
    if (dashboardData) {
        renderDashboard(dashboardData);
    }
}

async function loadProgress() {
    const result = await fetchApi('progress');
    if (result && result.data) {
        progressData = result.data;
        renderTables(progressData);
        renderDashboardTables(progressData);
    }
}

async function loadRowErrors() {
    const result = await fetchApi('row-errors');
    if (result && result.data) {
        rowErrorsData = result.data;
        renderRowErrors(rowErrorsData);
        updateRowErrorBadge(rowErrorsData.totalRowErrors);
    }
}

async function loadErrorLog() {
    const result = await fetchApi('error-log');
    if (result && result.data) {
        errorLogData = result.data;
        renderErrorLog(errorLogData);
        updateErrorBadge(errorLogData.totalEntries);
        renderDashboardRecentErrors(errorLogData);
    }
}

async function loadValidation() {
    const result = await fetchApi('validation');
    if (result && result.data) {
        validationData = result.data;
        renderValidation(validationData);
        renderDashboardValidation(validationData);
    }
}

async function loadMigrations() {
    const migrations = await fetchApi('migrations');
    if (migrations) {
        renderMigrationHistory(migrations);
    }
}

async function loadValidations() {
    const validations = await fetchApi('validations');
    if (validations) {
        renderValidationHistory(validations);
    }
}

async function loadAllData() {
    await Promise.all([
        loadDashboard(),
        loadProgress(),
        loadRowErrors(),
        loadErrorLog(),
        loadValidation(),
        loadMigrations(),
        loadValidations()
    ]);
}

/**
 * Auto-refresh Functions
 */
function startAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
    refreshInterval = setInterval(() => {
        if (autoRefresh) {
            loadAllData();
        }
    }, REFRESH_INTERVAL);
}

function stopAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }
}

/**
 * Server-Sent Events for real-time updates
 */
function connectSSE() {
    eventSource = new EventSource('/api/events');

    eventSource.onopen = () => {
        updateConnectionStatus(true);
    };

    eventSource.onerror = () => {
        updateConnectionStatus(false);
        // Attempt to reconnect after 5 seconds
        setTimeout(connectSSE, 5000);
    };

    eventSource.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            handleSSEMessage(data);
        } catch (e) {
            console.error('Error parsing SSE message:', e);
        }
    };
}

function handleSSEMessage(data) {
    switch (data.type) {
        case 'connected':
            updateConnectionStatus(true);
            break;
        case 'progress':
            loadProgress();
            loadDashboard();
            showNotification('Progress Updated', 'Migration progress has been updated.', 'info');
            break;
        case 'rowErrors':
            loadRowErrors();
            showNotification('Row Errors Updated', 'Row errors file has been updated.', 'warning');
            break;
        case 'errorLog':
            loadErrorLog();
            showNotification('Error Log Updated', 'Error log has been updated.', 'danger');
            break;
        case 'validation':
            loadValidation();
            loadValidations();
            showNotification('Validation Updated', 'Validation results have been updated.', 'info');
            break;
        case 'fileAdded':
            loadAllData();
            showNotification('File Added', `New file detected: ${data.data.file}`, 'info');
            break;
    }
}

function updateConnectionStatus(connected) {
    const statusEl = document.getElementById('connection-status');
    if (connected) {
        statusEl.className = 'badge connected';
        statusEl.innerHTML = '<i class="bi bi-circle-fill me-1"></i> Connected';
    } else {
        statusEl.className = 'badge disconnected';
        statusEl.innerHTML = '<i class="bi bi-circle-fill me-1"></i> Disconnected';
    }
}

/**
 * View Navigation
 */
function showView(viewName) {
    currentView = viewName;

    // Hide all views
    document.querySelectorAll('.view-content').forEach(view => {
        view.classList.add('d-none');
    });

    // Show selected view
    const selectedView = document.getElementById(`view-${viewName}`);
    if (selectedView) {
        selectedView.classList.remove('d-none');
    }

    // Update nav links
    document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
        if (link.dataset.view === viewName) {
            link.classList.add('active');
        }
    });

    // Load view-specific data if needed
    switch (viewName) {
        case 'history':
            loadMigrations();
            loadValidations();
            break;
    }
}

/**
 * Render Functions
 */
function renderDashboard(data) {
    // Migration status
    const statusEl = document.getElementById('migration-status');
    const nameEl = document.getElementById('migration-name');
    const statusIcon = document.getElementById('status-icon');

    statusEl.textContent = data.status;
    nameEl.textContent = data.migrationName;

    // Update status icon and color
    let iconClass, bgClass, textClass;
    switch (data.status) {
        case 'Completed':
            iconClass = 'bi-check-circle-fill';
            bgClass = 'bg-success';
            textClass = 'text-success';
            break;
        case 'InProgress':
            iconClass = 'bi-arrow-repeat';
            bgClass = 'bg-primary';
            textClass = 'text-primary';
            break;
        case 'Failed':
            iconClass = 'bi-x-circle-fill';
            bgClass = 'bg-danger';
            textClass = 'text-danger';
            break;
        default:
            iconClass = 'bi-hourglass-split';
            bgClass = 'bg-secondary';
            textClass = 'text-secondary';
    }

    statusIcon.className = `rounded-circle ${bgClass} bg-opacity-25 p-3`;
    statusIcon.innerHTML = `<i class="bi ${iconClass} fs-4 ${textClass}"></i>`;

    // Tables stats
    document.getElementById('tables-completed').textContent = formatNumber(data.tables.completed);
    document.getElementById('tables-total').textContent = formatNumber(data.tables.total);

    // Rows stats
    document.getElementById('rows-percentage').textContent = data.rows.percentage;
    document.getElementById('rows-processed').textContent = formatNumber(data.rows.processed);
    document.getElementById('rows-total').textContent = formatNumber(data.rows.total);

    // Progress bar
    const progressBar = document.getElementById('overall-progress-bar');
    const progressText = document.getElementById('progress-text');
    progressBar.style.width = `${data.rows.percentage}%`;
    progressBar.setAttribute('aria-valuenow', data.rows.percentage);
    progressText.textContent = `${data.rows.percentage}%`;

    // Update progress bar color based on status
    progressBar.className = 'progress-bar progress-bar-striped';
    if (data.status === 'InProgress') {
        progressBar.classList.add('progress-bar-animated', 'bg-primary');
    } else if (data.status === 'Completed') {
        progressBar.classList.add('bg-success');
    } else if (data.status === 'Failed') {
        progressBar.classList.add('bg-danger');
    } else {
        progressBar.classList.add('bg-secondary');
    }

    // Errors stats
    document.getElementById('total-errors').textContent = formatNumber(data.errors.logEntries);
    document.getElementById('row-errors-count').textContent = formatNumber(data.errors.rowErrors);

    const errorsIcon = document.getElementById('errors-icon');
    if (data.errors.logEntries > 0 || data.errors.rowErrors > 0) {
        errorsIcon.className = 'rounded-circle bg-danger bg-opacity-25 p-3';
        errorsIcon.innerHTML = '<i class="bi bi-exclamation-triangle fs-4 text-danger"></i>';
    } else {
        errorsIcon.className = 'rounded-circle bg-success bg-opacity-25 p-3';
        errorsIcon.innerHTML = '<i class="bi bi-check-circle fs-4 text-success"></i>';
    }

    // Timestamps
    document.getElementById('started-at').textContent = data.startedAt
        ? `Started: ${formatDateTime(data.startedAt)}`
        : 'Started: --';
    document.getElementById('last-updated').textContent = data.lastUpdatedAt
        ? `Last updated: ${formatDateTime(data.lastUpdatedAt)}`
        : 'Last updated: --';
}

function renderDashboardTables(data) {
    const container = document.getElementById('dashboard-tables-list');

    if (!data || !data.tables || data.tables.length === 0) {
        container.innerHTML = `
            <div class="list-group-item text-center text-muted py-4">
                <i class="bi bi-inbox fs-1 d-block mb-2"></i>
                No migration data available
            </div>
        `;
        return;
    }

    let html = '';
    data.tables.forEach(table => {
        const percentage = table.totalRows > 0
            ? Math.round((table.processedRows / table.totalRows) * 100)
            : 0;

        const statusBadge = getStatusBadge(table.status);
        const rowClass = `table-row-${table.status.toLowerCase().replace(' ', '-')}`;

        html += `
            <div class="list-group-item ${rowClass}">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="fw-medium">${table.sourceTable}</div>
                        <small class="text-muted">→ ${table.targetTable}</small>
                    </div>
                    <div class="text-end">
                        ${statusBadge}
                        <div class="mt-1">
                            <small class="text-muted">${formatNumber(table.processedRows)} / ${formatNumber(table.totalRows)}</small>
                        </div>
                        <div class="progress progress-mini mt-1">
                            <div class="progress-bar ${getProgressBarClass(table.status)}" 
                                 style="width: ${percentage}%"></div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
}

function renderDashboardRecentErrors(data) {
    const container = document.getElementById('dashboard-recent-errors');

    if (!data || !data.entries || data.entries.length === 0) {
        container.innerHTML = `
            <div class="list-group-item text-center text-muted py-4">
                <i class="bi bi-check-circle fs-1 d-block mb-2 text-success"></i>
                No errors
            </div>
        `;
        return;
    }

    // Show last 5 errors
    const recentErrors = data.entries.slice(-5).reverse();
    let html = '';

    recentErrors.forEach(error => {
        html += `
            <div class="list-group-item error-item">
                <div class="d-flex justify-content-between">
                    <small class="text-danger fw-medium">${error.table || 'General'}</small>
                    <small class="text-muted">${formatTime(error.timestamp)}</small>
                </div>
                <small class="text-muted text-truncate d-block">${escapeHtml(error.message)}</small>
            </div>
        `;
    });

    container.innerHTML = html;
}

function renderDashboardValidation(data) {
    const container = document.getElementById('validation-summary');

    if (!data) {
        container.innerHTML = '<p class="text-muted text-center mb-0">No validation data</p>';
        return;
    }

    const icon = data.isValid
        ? '<i class="bi bi-check-circle-fill text-success me-2"></i>'
        : '<i class="bi bi-x-circle-fill text-danger me-2"></i>';

    const statusText = data.isValid ? 'Valid' : 'Invalid';

    container.innerHTML = `
        <div class="text-center">
            ${icon}<strong>${statusText}</strong>
        </div>
        <div class="row mt-3 text-center">
            <div class="col-6">
                <div class="text-danger fw-bold">${data.summary?.errorsFound || 0}</div>
                <small class="text-muted">Errors</small>
            </div>
            <div class="col-6">
                <div class="text-warning fw-bold">${data.summary?.warningsFound || 0}</div>
                <small class="text-muted">Warnings</small>
            </div>
        </div>
        <div class="text-center mt-2">
            <small class="text-muted">${formatDateTime(data.validatedAt)}</small>
        </div>
    `;
}

function renderTables(data) {
    const tbody = document.getElementById('tables-tbody');

    if (!data || !data.tables || data.tables.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted py-4">
                    No table data available
                </td>
            </tr>
        `;
        return;
    }

    let html = '';
    data.tables.forEach(table => {
        const percentage = table.totalRows > 0
            ? Math.round((table.processedRows / table.totalRows) * 100)
            : 0;

        const statusBadge = getStatusBadge(table.status);
        const rowClass = `table-row-${table.status.toLowerCase().replace(' ', '-')}`;

        html += `
            <tr class="${rowClass}" data-status="${table.status}">
                <td>${statusBadge}</td>
                <td><code>${table.sourceTable}</code></td>
                <td><code>${table.targetTable}</code></td>
                <td>
                    <div class="d-flex align-items-center">
                        <div class="progress progress-mini me-2">
                            <div class="progress-bar ${getProgressBarClass(table.status)}" 
                                 style="width: ${percentage}%"></div>
                        </div>
                        <span>${percentage}%</span>
                    </div>
                </td>
                <td class="text-end">
                    <span class="fw-medium">${formatNumber(table.processedRows)}</span>
                    <span class="text-muted">/ ${formatNumber(table.totalRows)}</span>
                </td>
                <td>
                    ${table.lastBatchKeyValue
                        ? `<code class="small">${table.lastBatchKeyValue}</code>`
                        : '<span class="text-muted">--</span>'
                    }
                </td>
            </tr>
        `;
    });

    tbody.innerHTML = html;
}

function renderErrorLog(data) {
    const container = document.getElementById('error-log-list');
    const countEl = document.getElementById('error-log-count');

    countEl.textContent = `${data.totalEntries} entries`;

    if (!data.entries || data.entries.length === 0) {
        container.innerHTML = `
            <div class="list-group-item text-center text-muted py-4">
                <i class="bi bi-check-circle fs-1 d-block mb-2 text-success"></i>
                No error log entries
            </div>
        `;
        return;
    }

    let html = '';
    data.entries.slice().reverse().forEach(entry => {
        html += `
            <div class="list-group-item error-item">
                <div class="d-flex justify-content-between align-items-start">
                    <div>
                        <span class="badge bg-danger me-2">${entry.level}</span>
                        ${entry.table ? `<span class="badge bg-secondary">${entry.table}</span>` : ''}
                    </div>
                    <small class="text-muted time-display">${formatDateTime(entry.timestamp)}</small>
                </div>
                <div class="mt-2">
                    <span class="text-light">${escapeHtml(entry.message)}</span>
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
}

function renderRowErrors(data) {
    const container = document.getElementById('row-errors-accordion');
    const totalEl = document.getElementById('row-errors-total');

    totalEl.textContent = `${data.totalRowErrors} total errors`;

    if (!data.tables || data.tables.length === 0 || data.totalRowErrors === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <i class="bi bi-check-circle fs-1 d-block mb-2 text-success"></i>
                No row errors
            </div>
        `;
        return;
    }

    let html = '';
    data.tables.forEach((table, tableIndex) => {
        const tableId = `table-${tableIndex}`;

        html += `
            <div class="accordion-item">
                <h2 class="accordion-header">
                    <button class="accordion-button collapsed" type="button" 
                            data-bs-toggle="collapse" data-bs-target="#${tableId}">
                        <span class="me-2">
                            <code>${table.sourceTable}</code> → <code>${table.targetTable}</code>
                        </span>
                        <span class="badge bg-danger">${table.errorCount} errors</span>
                    </button>
                </h2>
                <div id="${tableId}" class="accordion-collapse collapse">
                    <div class="accordion-body p-0">
                        <div class="table-responsive">
                            <table class="table table-sm table-hover mb-0">
                                <thead class="table-dark">
                                    <tr>
                                        <th>Key</th>
                                        <th>Time</th>
                                        <th>Error</th>
                                        <th></th>
                                    </tr>
                                </thead>
                                <tbody>
        `;

        table.rows.forEach((row, rowIndex) => {
            const rowId = `${tableId}-row-${rowIndex}`;
            html += `
                <tr>
                    <td><code>${row.sourceKeyValue}</code></td>
                    <td class="time-display">${formatTime(row.errorTimestamp)}</td>
                    <td class="text-truncate" style="max-width: 300px;">${escapeHtml(row.errorMessage)}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-secondary" 
                                onclick="showRowErrorDetail('${tableIndex}', '${rowIndex}')">
                            <i class="bi bi-eye"></i>
                        </button>
                    </td>
                </tr>
            `;
        });

        html += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
}

function renderValidation(data) {
    const container = document.getElementById('validation-details');
    const badge = document.getElementById('validation-status-badge');

    if (!data) {
        container.innerHTML = '<p class="text-muted text-center">No validation data available</p>';
        badge.textContent = 'No validation';
        badge.className = 'badge bg-secondary';
        return;
    }

    // Update badge
    badge.textContent = data.isValid ? 'Valid' : 'Invalid';
    badge.className = data.isValid ? 'badge bg-success' : 'badge bg-danger';

    let html = `
        <div class="row mb-4">
            <div class="col-md-6">
                <div class="text-center mb-3">
                    <i class="bi ${data.isValid ? 'bi-check-circle-fill validation-icon valid' : 'bi-x-circle-fill validation-icon invalid'}"></i>
                </div>
                <h4 class="text-center">${data.isValid ? 'Configuration Valid' : 'Configuration Invalid'}</h4>
                <p class="text-center text-muted">Validated: ${formatDateTime(data.validatedAt)}</p>
            </div>
            <div class="col-md-6">
                <div class="row text-center">
                    <div class="col-4">
                        <div class="fs-3 fw-bold text-primary">${data.summary?.tablesValidated || 0}</div>
                        <small class="text-muted">Tables</small>
                    </div>
                    <div class="col-4">
                        <div class="fs-3 fw-bold text-danger">${data.summary?.errorsFound || 0}</div>
                        <small class="text-muted">Errors</small>
                    </div>
                    <div class="col-4">
                        <div class="fs-3 fw-bold text-warning">${data.summary?.warningsFound || 0}</div>
                        <small class="text-muted">Warnings</small>
                    </div>
                </div>
            </div>
        </div>
    `;

    // Connection status
    if (data.connections) {
        html += `
            <div class="card mb-4">
                <div class="card-header">
                    <h6 class="mb-0"><i class="bi bi-plug me-2"></i>Connections</h6>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <h6>Source</h6>
                            <p class="mb-1">
                                ${data.connections.source?.isValid
                                    ? '<i class="bi bi-check-circle text-success me-1"></i>'
                                    : '<i class="bi bi-x-circle text-danger me-1"></i>'
                                }
                                <strong>${data.connections.source?.provider}</strong> - ${data.connections.source?.server}
                            </p>
                            <small class="text-muted">${data.connections.source?.message}</small>
                        </div>
                        <div class="col-md-6">
                            <h6>Target</h6>
                            <p class="mb-1">
                                ${data.connections.target?.isValid
                                    ? '<i class="bi bi-check-circle text-success me-1"></i>'
                                    : '<i class="bi bi-x-circle text-danger me-1"></i>'
                                }
                                <strong>${data.connections.target?.provider}</strong> - ${data.connections.target?.server}
                            </p>
                            <small class="text-muted">${data.connections.target?.message}</small>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    // Tables validation
    if (data.tables && data.tables.length > 0) {
        html += `
            <div class="card">
                <div class="card-header">
                    <h6 class="mb-0"><i class="bi bi-table me-2"></i>Table Validation</h6>
                </div>
                <div class="card-body p-0">
                    <div class="accordion" id="validationTablesAccordion">
        `;

        data.tables.forEach((table, index) => {
            const tableId = `validation-table-${index}`;
            const hasErrors = table.errors && table.errors.length > 0;
            const hasWarnings = table.warnings && table.warnings.length > 0;

            html += `
                <div class="accordion-item">
                    <h2 class="accordion-header">
                        <button class="accordion-button collapsed" type="button" 
                                data-bs-toggle="collapse" data-bs-target="#${tableId}">
                            ${table.isValid
                                ? '<i class="bi bi-check-circle text-success me-2"></i>'
                                : '<i class="bi bi-x-circle text-danger me-2"></i>'
                            }
                            <code class="me-2">${table.sourceTable}</code>
                            <span class="text-muted me-2">→</span>
                            <code>${table.targetTable}</code>
                            <span class="ms-auto me-2">
                                ${hasErrors ? `<span class="badge bg-danger">${table.errors.length} errors</span>` : ''}
                                ${hasWarnings ? `<span class="badge bg-warning text-dark">${table.warnings.length} warnings</span>` : ''}
                            </span>
                            <span class="badge bg-secondary">${formatNumber(table.sourceRowCount)} rows</span>
                        </button>
                    </h2>
                    <div id="${tableId}" class="accordion-collapse collapse">
                        <div class="accordion-body">
            `;

            // Errors
            if (hasErrors) {
                html += `
                    <h6 class="text-danger"><i class="bi bi-exclamation-circle me-1"></i>Errors</h6>
                    <ul class="list-unstyled mb-3">
                        ${table.errors.map(e => `<li class="error-item ps-3 py-1 mb-1">${escapeHtml(e)}</li>`).join('')}
                    </ul>
                `;
            }

            // Warnings
            if (hasWarnings) {
                html += `
                    <h6 class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>Warnings</h6>
                    <ul class="list-unstyled mb-3">
                        ${table.warnings.map(w => `<li class="warning-item ps-3 py-1 mb-1">${escapeHtml(w)}</li>`).join('')}
                    </ul>
                `;
            }

            // Sample data
            if (table.sampleData && table.sampleData.length > 0) {
                html += `
                    <h6><i class="bi bi-table me-1"></i>Sample Data</h6>
                    <div class="table-responsive">
                        <table class="table table-sm sample-data-table">
                            <thead class="table-dark">
                                <tr>
                                    <th>Source</th>
                                    <th>→</th>
                                    <th>Transformed</th>
                                </tr>
                            </thead>
                            <tbody>
                `;

                table.sampleData.forEach(sample => {
                    html += `
                        <tr>
                            <td><pre class="json-display mb-0">${JSON.stringify(sample.source, null, 2)}</pre></td>
                            <td class="text-muted text-center">→</td>
                            <td><pre class="json-display mb-0">${JSON.stringify(sample.transformed, null, 2)}</pre></td>
                        </tr>
                    `;
                });

                html += `
                            </tbody>
                        </table>
                    </div>
                `;
            }

            html += `
                        </div>
                    </div>
                </div>
            `;
        });

        html += `
                    </div>
                </div>
            </div>
        `;
    }

    container.innerHTML = html;
}

function renderMigrationHistory(migrations) {
    const container = document.getElementById('migration-history-list');

    if (!migrations || migrations.length === 0) {
        container.innerHTML = `
            <div class="list-group-item text-center text-muted py-4">
                No migration history
            </div>
        `;
        return;
    }

    let html = '';
    migrations.forEach(migration => {
        html += `
            <a href="#" class="list-group-item list-group-item-action" onclick="loadMigrationRun('${migration.runId}')">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="fw-medium">${migration.migrationName}</div>
                        <small class="text-muted">${formatDateTime(migration.startedAt)}</small>
                    </div>
                    <div class="text-end">
                        ${getStatusBadge(migration.status)}
                        <div class="mt-1">
                            <small class="text-muted">${migration.tableCount} tables</small>
                        </div>
                    </div>
                </div>
            </a>
        `;
    });

    container.innerHTML = html;
}

function renderValidationHistory(validations) {
    const container = document.getElementById('validation-history-list');

    if (!validations || validations.length === 0) {
        container.innerHTML = `
            <div class="list-group-item text-center text-muted py-4">
                No validation history
            </div>
        `;
        return;
    }

    let html = '';
    validations.forEach(validation => {
        html += `
            <a href="#" class="list-group-item list-group-item-action" onclick="loadValidationRun('${validation.runId}')">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="fw-medium">${validation.migrationName}</div>
                        <small class="text-muted">${formatDateTime(validation.validatedAt)}</small>
                    </div>
                    <div>
                        ${validation.isValid
                            ? '<span class="badge bg-success">Valid</span>'
                            : '<span class="badge bg-danger">Invalid</span>'
                        }
                    </div>
                </div>
            </a>
        `;
    });

    container.innerHTML = html;
}

/**
 * Helper Functions
 */
function getStatusBadge(status) {
    const statusClasses = {
        'Completed': 'status-completed',
        'InProgress': 'status-in-progress',
        'Pending': 'status-pending',
        'Failed': 'status-failed'
    };

    const statusClass = statusClasses[status] || 'status-pending';
    const displayText = status === 'InProgress' ? 'In Progress' : status;

    return `<span class="badge status-badge ${statusClass}">${displayText}</span>`;
}

function getProgressBarClass(status) {
    switch (status) {
        case 'Completed': return 'bg-success';
        case 'InProgress': return 'bg-primary progress-bar-striped progress-bar-animated';
        case 'Failed': return 'bg-danger';
        default: return 'bg-secondary';
    }
}

function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    return num.toLocaleString();
}

function formatDateTime(dateStr) {
    if (!dateStr) return '--';
    try {
        const date = new Date(dateStr);
        return date.toLocaleString();
    } catch {
        return dateStr;
    }
}

function formatTime(dateStr) {
    if (!dateStr) return '--';
    try {
        const date = new Date(dateStr);
        return date.toLocaleTimeString();
    } catch {
        return dateStr;
    }
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * User Interactions
 */
function filterTables(status) {
    // Update button states
    document.querySelectorAll('#view-tables .btn-group .btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');

    // Filter table rows
    const rows = document.querySelectorAll('#tables-tbody tr');
    rows.forEach(row => {
        if (status === 'all' || row.dataset.status === status) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

function showRowErrorDetail(tableIndex, rowIndex) {
    if (!rowErrorsData || !rowErrorsData.tables) return;

    const table = rowErrorsData.tables[tableIndex];
    const row = table.rows[rowIndex];

    const content = document.getElementById('row-error-detail-content');
    content.innerHTML = `
        <div class="mb-3">
            <h6>Table</h6>
            <p class="mb-1"><strong>Source:</strong> <code>${table.sourceTable}</code></p>
            <p class="mb-0"><strong>Target:</strong> <code>${table.targetTable}</code></p>
        </div>
        <div class="mb-3">
            <h6>Error Details</h6>
            <p class="mb-1"><strong>Source Key:</strong> <code>${row.sourceKeyValue}</code></p>
            <p class="mb-1"><strong>Timestamp:</strong> ${formatDateTime(row.errorTimestamp)}</p>
            <p class="mb-0"><strong>Message:</strong></p>
            <div class="alert alert-danger">${escapeHtml(row.errorMessage)}</div>
        </div>
        <div>
            <h6>Source Data</h6>
            <pre class="json-display">${JSON.stringify(row.sourceData, null, 2)}</pre>
        </div>
    `;

    const modal = new bootstrap.Modal(document.getElementById('rowErrorModal'));
    modal.show();
}

function exportRowErrors() {
    if (!rowErrorsData) {
        showNotification('Export Failed', 'No row errors data to export.', 'warning');
        return;
    }

    const blob = new Blob([JSON.stringify(rowErrorsData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `row-errors-${rowErrorsData.migrationRunId}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showNotification('Export Complete', 'Row errors have been exported.', 'success');
}

function loadMigrationRun(runId) {
    // Load specific migration run
    fetchApi(`progress/${runId}`).then(result => {
        if (result && result.data) {
            progressData = result.data;
            renderTables(progressData);
            renderDashboardTables(progressData);
            showView('tables');
        }
    });
}

function loadValidationRun(runId) {
    fetchApi(`validation/${runId}`).then(result => {
        if (result && result.data) {
            validationData = result.data;
            renderValidation(validationData);
            showView('validation');
        }
    });
}

function updateErrorBadge(count) {
    const badge = document.getElementById('error-badge');
    if (count > 0) {
        badge.textContent = count;
        badge.classList.remove('d-none');
    } else {
        badge.classList.add('d-none');
    }
}

function updateRowErrorBadge(count) {
    const badge = document.getElementById('row-error-badge');
    if (count > 0) {
        badge.textContent = count;
        badge.classList.remove('d-none');
    } else {
        badge.classList.add('d-none');
    }
}

/**
 * Notifications
 */
function showNotification(title, message, type = 'info') {
    const toast = document.getElementById('notification-toast');
    const toastTitle = document.getElementById('toast-title');
    const toastBody = document.getElementById('toast-body');
    const toastIcon = document.getElementById('toast-icon');
    const toastTime = document.getElementById('toast-time');

    toastTitle.textContent = title;
    toastBody.textContent = message;
    toastTime.textContent = 'just now';

    // Set icon based on type
    const icons = {
        'info': 'bi-info-circle text-info',
        'success': 'bi-check-circle text-success',
        'warning': 'bi-exclamation-triangle text-warning',
        'danger': 'bi-x-circle text-danger'
    };
    toastIcon.className = `bi ${icons[type] || icons.info} me-2`;

    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
}
