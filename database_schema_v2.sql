# EPCM Project Scorecard v2.0 - Complete Database Schema
# SQLite database structure for professional project controls

"""
This schema is designed for:
- 2-5 concurrent projects
- Deliverable-based budgeting with rollups
- Dual forecasting (deliverable FTC + manning)
- Change order tracking with flexible gates
- PO/Invoice tracking with accruals
- Weekly PDF reporting
"""

# ============================================================================
# CORE TABLES
# ============================================================================

CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    client TEXT NOT NULL,
    project_type TEXT,
    start_date TEXT,
    end_date TEXT,
    report_date TEXT,
    contract_value REAL DEFAULT 0,
    contingency_pct REAL DEFAULT 10,
    status TEXT DEFAULT 'active',  -- active, on_hold, complete, archived
    created_date TEXT,
    created_by TEXT,
    notes TEXT
);

# ============================================================================
# DELIVERABLES & BUDGET (Critical - Budget by deliverable)
# ============================================================================

CREATE TABLE IF NOT EXISTS deliverables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    wbs_code TEXT,
    deliverable_name TEXT NOT NULL,
    discipline TEXT NOT NULL,  -- ME, EE, IC, ST, CIVIL, GN
    function TEXT NOT NULL,  -- MANAGEMENT, ENGINEERING, DRAFTING
    
    -- Budget
    budget_hours REAL NOT NULL DEFAULT 0,
    
    -- Progress tracking
    status TEXT DEFAULT 'not_started',  -- not_started, in_progress, internal_review, client_review, issued, complete
    physical_progress REAL DEFAULT 0,  -- 0-100 %
    manual_progress_override INTEGER DEFAULT 0,  -- Boolean: use manual instead of status-based
    earned_hours REAL DEFAULT 0,  -- Manual override for earned value
    
    -- Forecast
    forecast_to_complete REAL DEFAULT 0,  -- Hours to complete from this deliverable
    
    -- Dates
    planned_start TEXT,
    planned_complete TEXT,
    actual_start TEXT,
    actual_complete TEXT,
    
    -- Links
    parent_deliverable_id INTEGER,  -- For sub-deliverables
    
    created_date TEXT,
    modified_date TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_deliverable_id) REFERENCES deliverables(id)
);

-- Progress gates with configurable % complete
CREATE TABLE IF NOT EXISTS progress_gates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,  -- NULL = global default
    gate_name TEXT NOT NULL,
    default_progress_pct REAL NOT NULL,  -- Default % for this gate
    sort_order INTEGER,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

-- Default gates (inserted on project creation)
INSERT INTO progress_gates (project_id, gate_name, default_progress_pct, sort_order) VALUES
(NULL, 'Not Started', 0, 1),
(NULL, 'In Progress', 25, 2),
(NULL, 'Internal Review', 75, 3),
(NULL, 'Client Review', 85, 4),
(NULL, 'Issued for Construction', 95, 5),
(NULL, 'Complete', 100, 6);

# ============================================================================
# CHANGE MANAGEMENT
# ============================================================================

CREATE TABLE IF NOT EXISTS change_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    co_number TEXT NOT NULL,
    description TEXT NOT NULL,
    
    -- Classification
    change_type TEXT NOT NULL,  -- client_change, internal, design_change, constructability
    client_billable INTEGER DEFAULT 0,  -- 0=absorb, 1=bill to client
    
    -- Status workflow
    status TEXT DEFAULT 'draft',  -- draft, submitted, approved, rejected, incorporated
    
    -- Hours impact (estimate)
    hours_mgmt REAL DEFAULT 0,
    hours_eng REAL DEFAULT 0,
    hours_draft REAL DEFAULT 0,
    total_hours REAL DEFAULT 0,
    
    -- Cost impact
    estimated_cost REAL DEFAULT 0,
    approved_cost REAL DEFAULT 0,
    fee_recovery REAL DEFAULT 0,  -- If client_billable
    
    -- Dates
    created_date TEXT,
    submitted_date TEXT,
    approval_date TEXT,
    incorporated_date TEXT,
    
    -- Links
    linked_deliverables TEXT,  -- JSON array of deliverable IDs affected
    
    -- Approval tracking
    approved_by TEXT,
    approval_notes TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

# ============================================================================
# BUDGET MANAGEMENT
# ============================================================================

-- Budget transfers between functions/disciplines
CREATE TABLE IF NOT EXISTS budget_transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    transfer_date TEXT NOT NULL,
    
    from_function TEXT,
    from_discipline TEXT,
    to_function TEXT,
    to_discipline TEXT,
    
    hours REAL NOT NULL,
    reason TEXT NOT NULL,
    
    -- Can also transfer from specific deliverable
    from_deliverable_id INTEGER,
    to_deliverable_id INTEGER,
    
    approved_by TEXT,
    created_date TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (from_deliverable_id) REFERENCES deliverables(id),
    FOREIGN KEY (to_deliverable_id) REFERENCES deliverables(id)
);

-- Contingency tracking
CREATE TABLE IF NOT EXISTS contingency_drawdowns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    drawdown_date TEXT NOT NULL,
    hours REAL NOT NULL,
    reason TEXT NOT NULL,
    allocated_to_deliverable_id INTEGER,  -- Where contingency was used
    approved_by TEXT,
    created_date TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (allocated_to_deliverable_id) REFERENCES deliverables(id)
);

# ============================================================================
# EXTERNAL COSTS (POs & Invoices)
# ============================================================================

CREATE TABLE IF NOT EXISTS purchase_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    po_number TEXT NOT NULL,
    supplier TEXT NOT NULL,
    description TEXT NOT NULL,
    
    category TEXT,  -- equipment, services, materials, subcontract
    
    commitment_value REAL NOT NULL DEFAULT 0,
    
    -- Tracking
    invoiced_to_date REAL DEFAULT 0,
    accrued_work_done REAL DEFAULT 0,  -- Work done but not invoiced
    remaining_commitment REAL DEFAULT 0,  -- Auto-calculated
    
    status TEXT DEFAULT 'issued',  -- issued, partially_invoiced, fully_invoiced, closed
    
    issue_date TEXT,
    expected_completion_date TEXT,
    close_date TEXT,
    
    -- Links
    linked_deliverables TEXT,  -- JSON array
    
    notes TEXT,
    created_date TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER NOT NULL,
    invoice_number TEXT NOT NULL,
    invoice_date TEXT NOT NULL,
    
    amount REAL NOT NULL,
    
    payment_status TEXT DEFAULT 'received',  -- received, under_review, approved, paid
    
    due_date TEXT,
    paid_date TEXT,
    payment_reference TEXT,
    
    notes TEXT,
    created_date TEXT,
    
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE
);

# ============================================================================
# TIMESHEETS & ACTUALS
# ============================================================================

CREATE TABLE IF NOT EXISTS timesheets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    
    date TEXT NOT NULL,
    staff_name TEXT NOT NULL,
    task_name TEXT,
    
    hours REAL NOT NULL,
    
    -- Categorization
    function TEXT NOT NULL,  -- MANAGEMENT, ENGINEERING, DRAFTING
    discipline TEXT,  -- ME, EE, IC, etc
    
    -- Costing
    position TEXT,  -- From staff table
    rate REAL NOT NULL,
    cost REAL NOT NULL,  -- hours * rate
    
    -- Grouping
    week_ending TEXT NOT NULL,
    
    -- Optional link to deliverable (if tracked at that level)
    deliverable_id INTEGER,
    
    -- Import tracking
    import_batch_id TEXT,
    import_date TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (deliverable_id) REFERENCES deliverables(id)
);

# ============================================================================
# MANNING FORECAST (Bums on Seats)
# ============================================================================

CREATE TABLE IF NOT EXISTS manning_forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    
    person_name TEXT NOT NULL,
    position TEXT NOT NULL,
    discipline TEXT NOT NULL,
    function TEXT NOT NULL,
    
    week_ending TEXT NOT NULL,
    forecast_hours REAL NOT NULL,
    
    -- Costing (from position rate at time of forecast)
    hourly_rate REAL NOT NULL,
    forecast_cost REAL NOT NULL,
    
    created_date TEXT,
    modified_date TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    
    UNIQUE(project_id, person_name, week_ending)  -- One entry per person per week
);

# ============================================================================
# MASTER DATA (Global, not project-specific)
# ============================================================================

CREATE TABLE IF NOT EXISTS staff (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    function TEXT NOT NULL,
    discipline TEXT NOT NULL,
    position TEXT NOT NULL,
    active INTEGER DEFAULT 1,
    start_date TEXT,
    end_date TEXT
);

CREATE TABLE IF NOT EXISTS rate_schedule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position TEXT NOT NULL,
    rate REAL NOT NULL,
    effective_date TEXT NOT NULL,
    end_date TEXT,
    
    -- Multiple rates per position over time
    UNIQUE(position, effective_date)
);

# ============================================================================
# HISTORICAL TRACKING & SNAPSHOTS
# ============================================================================

CREATE TABLE IF NOT EXISTS weekly_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    snapshot_date TEXT NOT NULL,
    week_ending TEXT NOT NULL,
    
    -- Snapshot data stored as JSON
    project_state TEXT NOT NULL,  -- Full project state
    deliverable_state TEXT NOT NULL,  -- All deliverables state
    forecast_state TEXT NOT NULL,  -- FTC at that time
    
    -- Key metrics at time of snapshot
    budget_hours REAL,
    actual_hours REAL,
    earned_hours REAL,
    forecast_to_complete REAL,
    forecast_at_completion REAL,
    
    created_by TEXT,
    notes TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    
    UNIQUE(project_id, snapshot_date)
);

# ============================================================================
# REPORTING & COMMENTARY
# ============================================================================

CREATE TABLE IF NOT EXISTS weekly_commentary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    week_ending TEXT NOT NULL,
    
    key_activities TEXT,  -- This week's activities
    next_period_activities TEXT,  -- Next week's plan
    issues_risks TEXT,  -- Issues and risks
    general_notes TEXT,  -- Additional commentary
    
    -- Variance explanations
    schedule_variance_notes TEXT,
    cost_variance_notes TEXT,
    forecast_change_notes TEXT,
    
    created_date TEXT,
    created_by TEXT,
    
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    
    UNIQUE(project_id, week_ending)
);

# ============================================================================
# REFERENCE DATA
# ============================================================================

-- Discipline reference
CREATE TABLE IF NOT EXISTS disciplines (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    function TEXT NOT NULL,  -- MANAGEMENT, ENGINEERING, DRAFTING
    active INTEGER DEFAULT 1
);

-- Default disciplines
INSERT INTO disciplines (code, name, function) VALUES
('GN', 'General/Management', 'MANAGEMENT'),
('ME', 'Mechanical', 'ENGINEERING'),
('EE', 'Electrical', 'ENGINEERING'),
('IC', 'Instrumentation & Control', 'ENGINEERING'),
('ST', 'Structural', 'ENGINEERING'),
('STCC', 'Structural Steel Checker', 'ENGINEERING'),
('CIVIL', 'Civil', 'ENGINEERING'),
('PROC', 'Process', 'ENGINEERING'),
('PIPING', 'Piping', 'ENGINEERING'),
('ARCH', 'Architectural', 'ENGINEERING'),
('HVAC', 'HVAC', 'ENGINEERING'),
('CAD', 'CAD/Drafting', 'DRAFTING'),
('3D', '3D Modeling', 'DRAFTING');

# ============================================================================
# INDEXES FOR PERFORMANCE
# ============================================================================

CREATE INDEX IF NOT EXISTS idx_deliverables_project ON deliverables(project_id);
CREATE INDEX IF NOT EXISTS idx_deliverables_discipline ON deliverables(discipline);
CREATE INDEX IF NOT EXISTS idx_deliverables_function ON deliverables(function);

CREATE INDEX IF NOT EXISTS idx_timesheets_project ON timesheets(project_id);
CREATE INDEX IF NOT EXISTS idx_timesheets_week ON timesheets(week_ending);
CREATE INDEX IF NOT EXISTS idx_timesheets_deliverable ON timesheets(deliverable_id);

CREATE INDEX IF NOT EXISTS idx_manning_project ON manning_forecast(project_id);
CREATE INDEX IF NOT EXISTS idx_manning_week ON manning_forecast(week_ending);

CREATE INDEX IF NOT EXISTS idx_change_orders_project ON change_orders(project_id);
CREATE INDEX IF NOT EXISTS idx_pos_project ON purchase_orders(project_id);

CREATE INDEX IF NOT EXISTS idx_snapshots_project_date ON weekly_snapshots(project_id, snapshot_date);

# ============================================================================
# VIEWS FOR COMMON QUERIES
# ============================================================================

-- Project summary view
CREATE VIEW IF NOT EXISTS v_project_summary AS
SELECT 
    p.id,
    p.name,
    p.client,
    p.status,
    
    -- Budget (from deliverables)
    COALESCE(SUM(d.budget_hours), 0) as budget_hours,
    
    -- Actuals (from timesheets)
    COALESCE(
        (SELECT SUM(hours) FROM timesheets WHERE project_id = p.id), 
        0
    ) as actual_hours,
    
    -- Earned (from deliverables)
    COALESCE(
        (SELECT SUM(
            CASE 
                WHEN manual_progress_override = 1 THEN earned_hours
                ELSE budget_hours * physical_progress / 100.0
            END
        ) FROM deliverables WHERE project_id = p.id),
        0
    ) as earned_hours,
    
    -- Forecast
    COALESCE(
        (SELECT SUM(forecast_to_complete) FROM deliverables WHERE project_id = p.id),
        0
    ) as forecast_to_complete
    
FROM projects p
LEFT JOIN deliverables d ON d.project_id = p.id
GROUP BY p.id;

-- Weekly spend view
CREATE VIEW IF NOT EXISTS v_weekly_spend AS
SELECT 
    project_id,
    week_ending,
    function,
    discipline,
    SUM(hours) as hours,
    SUM(cost) as cost,
    COUNT(DISTINCT staff_name) as staff_count
FROM timesheets
GROUP BY project_id, week_ending, function, discipline;

-- Deliverable progress view
CREATE VIEW IF NOT EXISTS v_deliverable_progress AS
SELECT 
    d.id,
    d.project_id,
    d.wbs_code,
    d.deliverable_name,
    d.discipline,
    d.function,
    d.budget_hours,
    d.physical_progress,
    d.status,
    
    -- Earned hours
    CASE 
        WHEN d.manual_progress_override = 1 THEN d.earned_hours
        ELSE d.budget_hours * d.physical_progress / 100.0
    END as earned_hours,
    
    -- Actual hours spent
    COALESCE(
        (SELECT SUM(hours) FROM timesheets WHERE deliverable_id = d.id),
        0
    ) as actual_hours,
    
    -- Variance
    CASE 
        WHEN d.manual_progress_override = 1 THEN d.earned_hours
        ELSE d.budget_hours * d.physical_progress / 100.0
    END - COALESCE(
        (SELECT SUM(hours) FROM timesheets WHERE deliverable_id = d.id),
        0
    ) as variance_hours
    
FROM deliverables d;

-- Forecast reconciliation view
CREATE VIEW IF NOT EXISTS v_forecast_reconciliation AS
SELECT 
    p.id as project_id,
    p.name as project_name,
    
    -- Deliverable-based forecast
    COALESCE(SUM(d.forecast_to_complete), 0) as deliverable_ftc,
    
    -- Manning-based forecast
    COALESCE(
        (SELECT SUM(forecast_hours) 
         FROM manning_forecast 
         WHERE project_id = p.id 
         AND week_ending > date('now')),
        0
    ) as manning_ftc,
    
    -- Variance
    COALESCE(SUM(d.forecast_to_complete), 0) - 
    COALESCE(
        (SELECT SUM(forecast_hours) 
         FROM manning_forecast 
         WHERE project_id = p.id 
         AND week_ending > date('now')),
        0
    ) as forecast_variance
    
FROM projects p
LEFT JOIN deliverables d ON d.project_id = p.id
GROUP BY p.id;
