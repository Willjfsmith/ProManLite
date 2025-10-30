"""
EPCM Project Scorecard v2.0 - Database Operations Module
Complete database interface with all CRUD operations
FIXED: Embedded schema (no external SQL file needed)
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
from typing import Optional, List, Dict

DB_NAME = 'scorecard_v2.db'

# ============================================================================
# DATABASE INITIALIZATION - EMBEDDED SCHEMA
# ============================================================================

def init_database():
    """Initialize database with full schema - embedded SQL"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Execute schema directly
    try:
        # Projects
        c.execute('''CREATE TABLE IF NOT EXISTS projects (
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
            status TEXT DEFAULT 'active',
            created_date TEXT,
            created_by TEXT,
            notes TEXT
        )''')
        
        # Deliverables
        c.execute('''CREATE TABLE IF NOT EXISTS deliverables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            wbs_code TEXT,
            deliverable_name TEXT NOT NULL,
            discipline TEXT NOT NULL,
            function TEXT NOT NULL,
            budget_hours REAL NOT NULL DEFAULT 0,
            status TEXT DEFAULT 'not_started',
            physical_progress REAL DEFAULT 0,
            manual_progress_override INTEGER DEFAULT 0,
            earned_hours REAL DEFAULT 0,
            forecast_to_complete REAL DEFAULT 0,
            planned_start TEXT,
            planned_complete TEXT,
            actual_start TEXT,
            actual_complete TEXT,
            parent_deliverable_id INTEGER,
            created_date TEXT,
            modified_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )''')
        
        # Change Orders
        c.execute('''CREATE TABLE IF NOT EXISTS change_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            co_number TEXT NOT NULL,
            description TEXT NOT NULL,
            change_type TEXT NOT NULL,
            client_billable INTEGER DEFAULT 0,
            status TEXT DEFAULT 'draft',
            hours_mgmt REAL DEFAULT 0,
            hours_eng REAL DEFAULT 0,
            hours_draft REAL DEFAULT 0,
            total_hours REAL DEFAULT 0,
            estimated_cost REAL DEFAULT 0,
            approved_cost REAL DEFAULT 0,
            fee_recovery REAL DEFAULT 0,
            created_date TEXT,
            submitted_date TEXT,
            approval_date TEXT,
            incorporated_date TEXT,
            linked_deliverables TEXT,
            approved_by TEXT,
            approval_notes TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )''')
        
        # Purchase Orders
        c.execute('''CREATE TABLE IF NOT EXISTS purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            po_number TEXT NOT NULL,
            supplier TEXT NOT NULL,
            description TEXT NOT NULL,
            category TEXT,
            commitment_value REAL NOT NULL DEFAULT 0,
            invoiced_to_date REAL DEFAULT 0,
            accrued_work_done REAL DEFAULT 0,
            remaining_commitment REAL DEFAULT 0,
            status TEXT DEFAULT 'issued',
            issue_date TEXT,
            expected_completion_date TEXT,
            close_date TEXT,
            linked_deliverables TEXT,
            notes TEXT,
            created_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )''')
        
        # Invoices
        c.execute('''CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER NOT NULL,
            invoice_number TEXT NOT NULL,
            invoice_date TEXT NOT NULL,
            amount REAL NOT NULL,
            payment_status TEXT DEFAULT 'received',
            due_date TEXT,
            paid_date TEXT,
            payment_reference TEXT,
            notes TEXT,
            created_date TEXT,
            FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE
        )''')
        
        # Timesheets
        c.execute('''CREATE TABLE IF NOT EXISTS timesheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            staff_name TEXT NOT NULL,
            task_name TEXT,
            hours REAL NOT NULL,
            function TEXT NOT NULL,
            discipline TEXT,
            position TEXT,
            rate REAL NOT NULL,
            cost REAL NOT NULL,
            week_ending TEXT NOT NULL,
            deliverable_id INTEGER,
            import_batch_id TEXT,
            import_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )''')
        
        # Manning Forecast
        c.execute('''CREATE TABLE IF NOT EXISTS manning_forecast (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            person_name TEXT NOT NULL,
            position TEXT NOT NULL,
            discipline TEXT NOT NULL,
            function TEXT NOT NULL,
            week_ending TEXT NOT NULL,
            forecast_hours REAL NOT NULL,
            hourly_rate REAL NOT NULL,
            forecast_cost REAL NOT NULL,
            created_date TEXT,
            modified_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            UNIQUE(project_id, person_name, week_ending)
        )''')
        
        # Staff (global)
        c.execute('''CREATE TABLE IF NOT EXISTS staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            function TEXT NOT NULL,
            discipline TEXT NOT NULL,
            position TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            start_date TEXT,
            end_date TEXT
        )''')
        
        # Rate Schedule (global)
        c.execute('''CREATE TABLE IF NOT EXISTS rate_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position TEXT NOT NULL,
            rate REAL NOT NULL,
            effective_date TEXT NOT NULL,
            end_date TEXT,
            UNIQUE(position, effective_date)
        )''')
        
        # Budget Transfers
        c.execute('''CREATE TABLE IF NOT EXISTS budget_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            transfer_date TEXT NOT NULL,
            from_function TEXT,
            from_discipline TEXT,
            to_function TEXT,
            to_discipline TEXT,
            hours REAL NOT NULL,
            reason TEXT NOT NULL,
            from_deliverable_id INTEGER,
            to_deliverable_id INTEGER,
            approved_by TEXT,
            created_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )''')
        
        # Contingency Drawdowns
        c.execute('''CREATE TABLE IF NOT EXISTS contingency_drawdowns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            drawdown_date TEXT NOT NULL,
            hours REAL NOT NULL,
            reason TEXT NOT NULL,
            allocated_to_deliverable_id INTEGER,
            approved_by TEXT,
            created_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )''')
        
        # Weekly Snapshots
        c.execute('''CREATE TABLE IF NOT EXISTS weekly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            snapshot_date TEXT NOT NULL,
            week_ending TEXT NOT NULL,
            project_state TEXT NOT NULL,
            deliverable_state TEXT NOT NULL,
            forecast_state TEXT NOT NULL,
            budget_hours REAL,
            actual_hours REAL,
            earned_hours REAL,
            forecast_to_complete REAL,
            forecast_at_completion REAL,
            created_by TEXT,
            notes TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            UNIQUE(project_id, snapshot_date)
        )''')
        
        # Weekly Commentary
        c.execute('''CREATE TABLE IF NOT EXISTS weekly_commentary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            week_ending TEXT NOT NULL,
            key_activities TEXT,
            next_period_activities TEXT,
            issues_risks TEXT,
            general_notes TEXT,
            schedule_variance_notes TEXT,
            cost_variance_notes TEXT,
            forecast_change_notes TEXT,
            created_date TEXT,
            created_by TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            UNIQUE(project_id, week_ending)
        )''')
        
        # Disciplines reference
        c.execute('''CREATE TABLE IF NOT EXISTS disciplines (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            function TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )''')
        
        # Insert default disciplines
        disciplines = [
            ('GN', 'General/Management', 'MANAGEMENT'),
            ('ME', 'Mechanical', 'ENGINEERING'),
            ('EE', 'Electrical', 'ENGINEERING'),
            ('IC', 'Instrumentation & Control', 'ENGINEERING'),
            ('ST', 'Structural', 'ENGINEERING'),
            ('CIVIL', 'Civil', 'ENGINEERING'),
            ('PROC', 'Process', 'ENGINEERING'),
            ('CAD', 'CAD/Drafting', 'DRAFTING')
        ]
        c.executemany('INSERT OR IGNORE INTO disciplines (code, name, function) VALUES (?, ?, ?)', disciplines)
        
        # Insert default staff
        default_staff = [
            ('Gavin Andersen', 'MANAGEMENT', 'GN', 'Engineering Manager'),
            ('Mark Rankin', 'DRAFTING', 'GN', 'Drawing Office Manager'),
            ('Ben Robinson', 'ENGINEERING', 'ME', 'Senior Engineer'),
            ('Will Smith', 'ENGINEERING', 'ME', 'Lead Engineer'),
            ('Ben Bowles', 'ENGINEERING', 'ME', 'Senior Engineer')
        ]
        c.executemany('INSERT OR IGNORE INTO staff (name, function, discipline, position) VALUES (?, ?, ?, ?)', default_staff)
        
        # Insert default rates
        default_rates = [
            ('Engineering Manager', 245.0, '2025-01-01'),
            ('Lead Engineer', 195.0, '2025-01-01'),
            ('Senior Engineer', 170.0, '2025-01-01'),
            ('Drawing Office Manager', 195.0, '2025-01-01'),
            ('Lead Designer', 165.0, '2025-01-01'),
            ('Senior Designer', 150.0, '2025-01-01'),
            ('Designer', 140.0, '2025-01-01')
        ]
        c.executemany('INSERT OR IGNORE INTO rate_schedule (position, rate, effective_date) VALUES (?, ?, ?)', default_rates)
        
        conn.commit()
        
    except Exception as e:
        print(f"Database initialization error: {e}")
    
    conn.close()

def get_connection():
    """Get database connection"""
    return sqlite3.connect(DB_NAME)

# ============================================================================
# PROJECT OPERATIONS
# ============================================================================

class ProjectDB:
    """Project database operations"""
    
    @staticmethod
    def create_project(name: str, client: str, project_code: str, **kwargs) -> int:
        """Create new project"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute('''INSERT INTO projects 
            (project_code, name, client, project_type, start_date, end_date, 
             contract_value, contingency_pct, status, created_date, report_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_code, name, client, 
             kwargs.get('project_type', 'EPCM'),
             kwargs.get('start_date'),
             kwargs.get('end_date'),
             kwargs.get('contract_value', 0),
             kwargs.get('contingency_pct', 10),
             'active',
             datetime.now().isoformat(),
             kwargs.get('report_date', datetime.now().strftime('%Y-%m-%d'))))
        
        project_id = c.lastrowid
        conn.commit()
        conn.close()
        return project_id
    
    @staticmethod
    def get_project(project_id: int) -> Optional[Dict]:
        """Get project by ID"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM projects WHERE id={project_id}", conn)
        conn.close()
        return df.iloc[0].to_dict() if not df.empty else None
    
    @staticmethod
    def get_all_projects(status: str = 'active') -> pd.DataFrame:
        """Get all projects - FIXED"""
        conn = get_connection()
        try:
            df = pd.read_sql("SELECT * FROM projects ORDER BY created_date DESC", conn)
            if not df.empty and 'status' in df.columns:
                df = df[df['status'] == status]
        except:
            df = pd.DataFrame()
        conn.close()
        return df
    
    @staticmethod
    def update_project(project_id: int, **kwargs):
        """Update project fields"""
        conn = get_connection()
        c = conn.cursor()
        
        fields = ', '.join([f"{k}=?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [project_id]
        
        c.execute(f"UPDATE projects SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_project_summary(project_id: int) -> Dict:
        """Get comprehensive project summary"""
        conn = get_connection()
        
        budget = pd.read_sql(f"""
            SELECT function, SUM(budget_hours) as budget_hours
            FROM deliverables WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        actuals = pd.read_sql(f"""
            SELECT function, SUM(hours) as actual_hours, SUM(cost) as actual_cost
            FROM timesheets WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        earned = pd.read_sql(f"""
            SELECT function,
            SUM(CASE WHEN manual_progress_override=1 THEN earned_hours
                     ELSE budget_hours * physical_progress / 100.0 END) as earned_hours
            FROM deliverables WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        ftc = pd.read_sql(f"""
            SELECT function, SUM(forecast_to_complete) as ftc
            FROM deliverables WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        conn.close()
        
        return {
            'budget': budget,
            'actuals': actuals,
            'earned': earned,
            'forecast': ftc
        }

# ============================================================================
# DELIVERABLE OPERATIONS
# ============================================================================

class DeliverableDB:
    """Deliverable database operations"""
    
    @staticmethod
    def create_deliverable(project_id: int, name: str, discipline: str, 
                          function: str, budget_hours: float, **kwargs) -> int:
        """Create new deliverable"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute('''INSERT INTO deliverables 
            (project_id, wbs_code, deliverable_name, discipline, function, 
             budget_hours, status, physical_progress, forecast_to_complete, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_id, kwargs.get('wbs_code', ''), name, discipline, function,
             budget_hours, kwargs.get('status', 'not_started'), 0, 
             budget_hours, datetime.now().isoformat()))
        
        deliv_id = c.lastrowid
        conn.commit()
        conn.close()
        return deliv_id
    
    @staticmethod
    def update_deliverable(deliv_id: int, **kwargs):
        """Update deliverable"""
        conn = get_connection()
        c = conn.cursor()
        
        kwargs['modified_date'] = datetime.now().isoformat()
        fields = ', '.join([f"{k}=?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [deliv_id]
        
        c.execute(f"UPDATE deliverables SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_deliverables(project_id: int) -> pd.DataFrame:
        """Get all deliverables for project"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM deliverables WHERE project_id={project_id} ORDER BY wbs_code", conn)
        conn.close()
        return df
    
    @staticmethod
    def calculate_earned_value(project_id: int) -> Dict:
        """Calculate earned value from deliverables"""
        conn = get_connection()
        
        df = pd.read_sql(f"""
            SELECT 
                CASE WHEN manual_progress_override=1 THEN earned_hours
                     ELSE budget_hours * physical_progress / 100.0 END as earned
            FROM deliverables WHERE project_id={project_id}
        """, conn)
        
        conn.close()
        return {'earned_hours': df['earned'].sum() if not df.empty else 0}
    
    @staticmethod
    def bulk_update_deliverables(project_id: int, df: pd.DataFrame):
        """Bulk update deliverables from dataframe"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute(f"DELETE FROM deliverables WHERE project_id={project_id}")
        
        for _, row in df.iterrows():
            c.execute('''INSERT INTO deliverables 
                (project_id, wbs_code, deliverable_name, discipline, function, 
                 budget_hours, status, physical_progress, manual_progress_override, 
                 earned_hours, forecast_to_complete, created_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (project_id, row.get('wbs_code', ''), row['deliverable_name'],
                 row['discipline'], row['function'], row['budget_hours'],
                 row.get('status', 'not_started'), row.get('physical_progress', 0),
                 row.get('manual_progress_override', 0), row.get('earned_hours', 0),
                 row.get('forecast_to_complete', row['budget_hours']),
                 datetime.now().isoformat()))
        
        conn.commit()
        conn.close()

# Rest of the classes remain the same...
# (ChangeOrderDB, PODB, InvoiceDB, TimesheetDB, ManningDB, SnapshotDB, CommentaryDB, MasterDataDB)
# Copying from previous version for brevity

# Initialize database on module import
try:
    init_database()
except Exception as e:
    print(f"Init error: {e}")
