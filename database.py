"""
EPCM Project Scorecard v2.0 - Complete Database Module
All operations - embedded schema - ready for Streamlit Cloud
Replace your database.py with this file
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
from typing import Optional, List, Dict

DB_NAME = 'scorecard_v2.db'

def init_database():
    """Initialize database - embedded schema"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_code TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
            client TEXT NOT NULL, project_type TEXT, start_date TEXT, end_date TEXT, report_date TEXT,
            contract_value REAL DEFAULT 0, contingency_pct REAL DEFAULT 10, status TEXT DEFAULT 'active',
            created_date TEXT, created_by TEXT, notes TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS deliverables (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, wbs_code TEXT,
            deliverable_name TEXT NOT NULL, discipline TEXT NOT NULL, function TEXT NOT NULL,
            budget_hours REAL NOT NULL DEFAULT 0, status TEXT DEFAULT 'not_started',
            physical_progress REAL DEFAULT 0, manual_progress_override INTEGER DEFAULT 0,
            earned_hours REAL DEFAULT 0, forecast_to_complete REAL DEFAULT 0,
            planned_start TEXT, planned_complete TEXT, actual_start TEXT, actual_complete TEXT,
            parent_deliverable_id INTEGER, created_date TEXT, modified_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS change_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, co_number TEXT NOT NULL,
            description TEXT NOT NULL, change_type TEXT NOT NULL, client_billable INTEGER DEFAULT 0,
            status TEXT DEFAULT 'draft', hours_mgmt REAL DEFAULT 0, hours_eng REAL DEFAULT 0,
            hours_draft REAL DEFAULT 0, total_hours REAL DEFAULT 0, estimated_cost REAL DEFAULT 0,
            approved_cost REAL DEFAULT 0, fee_recovery REAL DEFAULT 0, created_date TEXT,
            submitted_date TEXT, approval_date TEXT, incorporated_date TEXT, linked_deliverables TEXT,
            approved_by TEXT, approval_notes TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, po_number TEXT NOT NULL,
            supplier TEXT NOT NULL, description TEXT NOT NULL, category TEXT,
            commitment_value REAL NOT NULL DEFAULT 0, invoiced_to_date REAL DEFAULT 0,
            accrued_work_done REAL DEFAULT 0, remaining_commitment REAL DEFAULT 0,
            status TEXT DEFAULT 'issued', issue_date TEXT, expected_completion_date TEXT,
            close_date TEXT, linked_deliverables TEXT, notes TEXT, created_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT, po_id INTEGER NOT NULL, invoice_number TEXT NOT NULL,
            invoice_date TEXT NOT NULL, amount REAL NOT NULL, payment_status TEXT DEFAULT 'received',
            due_date TEXT, paid_date TEXT, payment_reference TEXT, notes TEXT, created_date TEXT,
            FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS timesheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, date TEXT NOT NULL,
            staff_name TEXT NOT NULL, task_name TEXT, hours REAL NOT NULL, function TEXT NOT NULL,
            discipline TEXT, position TEXT, rate REAL NOT NULL, cost REAL NOT NULL,
            week_ending TEXT NOT NULL, deliverable_id INTEGER, import_batch_id TEXT, import_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS manning_forecast (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, person_name TEXT NOT NULL,
            position TEXT NOT NULL, discipline TEXT NOT NULL, function TEXT NOT NULL,
            week_ending TEXT NOT NULL, forecast_hours REAL NOT NULL, hourly_rate REAL NOT NULL,
            forecast_cost REAL NOT NULL, created_date TEXT, modified_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            UNIQUE(project_id, person_name, week_ending))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, function TEXT NOT NULL,
            discipline TEXT NOT NULL, position TEXT NOT NULL, active INTEGER DEFAULT 1,
            start_date TEXT, end_date TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS rate_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT, position TEXT NOT NULL, rate REAL NOT NULL,
            effective_date TEXT NOT NULL, end_date TEXT, UNIQUE(position, effective_date))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS budget_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, transfer_date TEXT NOT NULL,
            from_function TEXT, from_discipline TEXT, to_function TEXT, to_discipline TEXT,
            hours REAL NOT NULL, reason TEXT NOT NULL, from_deliverable_id INTEGER,
            to_deliverable_id INTEGER, approved_by TEXT, created_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS contingency_drawdowns (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, drawdown_date TEXT NOT NULL,
            hours REAL NOT NULL, reason TEXT NOT NULL, allocated_to_deliverable_id INTEGER,
            approved_by TEXT, created_date TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS weekly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, snapshot_date TEXT NOT NULL,
            week_ending TEXT NOT NULL, project_state TEXT NOT NULL, deliverable_state TEXT NOT NULL,
            forecast_state TEXT NOT NULL, budget_hours REAL, actual_hours REAL, earned_hours REAL,
            forecast_to_complete REAL, forecast_at_completion REAL, created_by TEXT, notes TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            UNIQUE(project_id, snapshot_date))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS weekly_commentary (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, week_ending TEXT NOT NULL,
            key_activities TEXT, next_period_activities TEXT, issues_risks TEXT, general_notes TEXT,
            schedule_variance_notes TEXT, cost_variance_notes TEXT, forecast_change_notes TEXT,
            created_date TEXT, created_by TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            UNIQUE(project_id, week_ending))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS disciplines (
            code TEXT PRIMARY KEY, name TEXT NOT NULL, function TEXT NOT NULL, active INTEGER DEFAULT 1)''')
        
        disciplines = [('GN', 'General/Management', 'MANAGEMENT'), ('ME', 'Mechanical', 'ENGINEERING'),
                      ('EE', 'Electrical', 'ENGINEERING'), ('IC', 'Instrumentation & Control', 'ENGINEERING'),
                      ('ST', 'Structural', 'ENGINEERING'), ('CIVIL', 'Civil', 'ENGINEERING'),
                      ('PROC', 'Process', 'ENGINEERING'), ('CAD', 'CAD/Drafting', 'DRAFTING')]
        c.executemany('INSERT OR IGNORE INTO disciplines (code, name, function) VALUES (?, ?, ?)', disciplines)
        
        default_staff = [('Gavin Andersen', 'MANAGEMENT', 'GN', 'Engineering Manager'),
                        ('Mark Rankin', 'DRAFTING', 'GN', 'Drawing Office Manager'),
                        ('Ben Robinson', 'ENGINEERING', 'ME', 'Senior Engineer'),
                        ('Will Smith', 'ENGINEERING', 'ME', 'Lead Engineer'),
                        ('Ben Bowles', 'ENGINEERING', 'ME', 'Senior Engineer')]
        c.executemany('INSERT OR IGNORE INTO staff (name, function, discipline, position) VALUES (?, ?, ?, ?)', default_staff)
        
        default_rates = [('Engineering Manager', 245.0, '2025-01-01'), ('Lead Engineer', 195.0, '2025-01-01'),
                        ('Senior Engineer', 170.0, '2025-01-01'), ('Drawing Office Manager', 195.0, '2025-01-01'),
                        ('Lead Designer', 165.0, '2025-01-01'), ('Senior Designer', 150.0, '2025-01-01'),
                        ('Designer', 140.0, '2025-01-01')]
        c.executemany('INSERT OR IGNORE INTO rate_schedule (position, rate, effective_date) VALUES (?, ?, ?)', default_rates)
        
        conn.commit()
    except Exception as e:
        print(f"DB init error: {e}")
    conn.close()

def get_connection():
    return sqlite3.connect(DB_NAME)

class ProjectDB:
    @staticmethod
    def create_project(name: str, client: str, project_code: str, **kwargs) -> int:
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT INTO projects (project_code, name, client, project_type, start_date, end_date, 
                     contract_value, contingency_pct, status, created_date, report_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_code, name, client, kwargs.get('project_type', 'EPCM'),
                   kwargs.get('start_date'), kwargs.get('end_date'), kwargs.get('contract_value', 0),
                   kwargs.get('contingency_pct', 10), 'active', datetime.now().isoformat(),
                   kwargs.get('report_date', datetime.now().strftime('%Y-%m-%d'))))
        project_id = c.lastrowid
        conn.commit()
        conn.close()
        return project_id
    
    @staticmethod
    def get_project(project_id: int) -> Optional[Dict]:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM projects WHERE id={project_id}", conn)
        conn.close()
        return df.iloc[0].to_dict() if not df.empty else None
    
    @staticmethod
    def get_all_projects(status: str = 'active') -> pd.DataFrame:
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
        conn = get_connection()
        c = conn.cursor()
        fields = ', '.join([f"{k}=?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [project_id]
        c.execute(f"UPDATE projects SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_project_summary(project_id: int) -> Dict:
        conn = get_connection()
        budget = pd.read_sql(f"SELECT function, SUM(budget_hours) as budget_hours FROM deliverables WHERE project_id={project_id} GROUP BY function", conn)
        actuals = pd.read_sql(f"SELECT function, SUM(hours) as actual_hours, SUM(cost) as actual_cost FROM timesheets WHERE project_id={project_id} GROUP BY function", conn)
        earned = pd.read_sql(f"SELECT function, SUM(CASE WHEN manual_progress_override=1 THEN earned_hours ELSE budget_hours * physical_progress / 100.0 END) as earned_hours FROM deliverables WHERE project_id={project_id} GROUP BY function", conn)
        ftc = pd.read_sql(f"SELECT function, SUM(forecast_to_complete) as ftc FROM deliverables WHERE project_id={project_id} GROUP BY function", conn)
        conn.close()
        return {'budget': budget, 'actuals': actuals, 'earned': earned, 'forecast': ftc}

class DeliverableDB:
    @staticmethod
    def create_deliverable(project_id: int, name: str, discipline: str, function: str, budget_hours: float, **kwargs) -> int:
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT INTO deliverables (project_id, wbs_code, deliverable_name, discipline, function, 
                     budget_hours, status, physical_progress, forecast_to_complete, created_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_id, kwargs.get('wbs_code', ''), name, discipline, function, budget_hours,
                   kwargs.get('status', 'not_started'), 0, budget_hours, datetime.now().isoformat()))
        deliv_id = c.lastrowid
        conn.commit()
        conn.close()
        return deliv_id
    
    @staticmethod
    def update_deliverable(deliv_id: int, **kwargs):
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
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM deliverables WHERE project_id={project_id} ORDER BY wbs_code", conn)
        conn.close()
        return df
    
    @staticmethod
    def calculate_earned_value(project_id: int) -> Dict:
        conn = get_connection()
        df = pd.read_sql(f"SELECT CASE WHEN manual_progress_override=1 THEN earned_hours ELSE budget_hours * physical_progress / 100.0 END as earned FROM deliverables WHERE project_id={project_id}", conn)
        conn.close()
        return {'earned_hours': df['earned'].sum() if not df.empty else 0}
    
    @staticmethod
    def bulk_update_deliverables(project_id: int, df: pd.DataFrame):
        conn = get_connection()
        c = conn.cursor()
        c.execute(f"DELETE FROM deliverables WHERE project_id={project_id}")
        for _, row in df.iterrows():
            c.execute('''INSERT INTO deliverables (project_id, wbs_code, deliverable_name, discipline, function, 
                         budget_hours, status, physical_progress, manual_progress_override, earned_hours, 
                         forecast_to_complete, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (project_id, row.get('wbs_code', ''), row['deliverable_name'], row['discipline'], 
                       row['function'], row['budget_hours'], row.get('status', 'not_started'),
                       row.get('physical_progress', 0), row.get('manual_progress_override', 0),
                       row.get('earned_hours', 0), row.get('forecast_to_complete', row['budget_hours']),
                       datetime.now().isoformat()))
        conn.commit()
        conn.close()

class ChangeOrderDB:
    @staticmethod
    def create_change_order(project_id: int, co_number: str, description: str, change_type: str, **kwargs) -> int:
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT INTO change_orders (project_id, co_number, description, change_type, status,
                     hours_mgmt, hours_eng, hours_draft, client_billable, estimated_cost, created_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_id, co_number, description, change_type, kwargs.get('status', 'draft'),
                   kwargs.get('hours_mgmt', 0), kwargs.get('hours_eng', 0), kwargs.get('hours_draft', 0),
                   kwargs.get('client_billable', 0), kwargs.get('estimated_cost', 0), datetime.now().isoformat()))
        co_id = c.lastrowid
        conn.commit()
        conn.close()
        return co_id
    
    @staticmethod
    def get_change_orders(project_id: int) -> pd.DataFrame:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM change_orders WHERE project_id={project_id} ORDER BY created_date DESC", conn)
        conn.close()
        return df
    
    @staticmethod
    def update_change_order(co_id: int, **kwargs):
        conn = get_connection()
        c = conn.cursor()
        fields = ', '.join([f"{k}=?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [co_id]
        c.execute(f"UPDATE change_orders SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()

class PODB:
    @staticmethod
    def create_po(project_id: int, po_number: str, supplier: str, description: str, commitment_value: float, **kwargs) -> int:
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT INTO purchase_orders (project_id, po_number, supplier, description, category,
                     commitment_value, status, issue_date, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_id, po_number, supplier, description, kwargs.get('category', 'services'),
                   commitment_value, 'issued', kwargs.get('issue_date', datetime.now().strftime('%Y-%m-%d')),
                   datetime.now().isoformat()))
        po_id = c.lastrowid
        conn.commit()
        conn.close()
        return po_id
    
    @staticmethod
    def get_purchase_orders(project_id: int) -> pd.DataFrame:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM purchase_orders WHERE project_id={project_id}", conn)
        conn.close()
        return df
    
    @staticmethod
    def update_po_accrual(po_id: int, accrued_work_done: float):
        conn = get_connection()
        c = conn.cursor()
        invoiced = pd.read_sql(f"SELECT SUM(amount) as total FROM invoices WHERE po_id={po_id}", conn).iloc[0]['total']
        invoiced = invoiced if invoiced else 0
        commitment = pd.read_sql(f"SELECT commitment_value FROM purchase_orders WHERE id={po_id}", conn).iloc[0]['commitment_value']
        remaining = commitment - invoiced - accrued_work_done
        c.execute('UPDATE purchase_orders SET accrued_work_done=?, remaining_commitment=? WHERE id=?',
                 (accrued_work_done, remaining, po_id))
        conn.commit()
        conn.close()

class InvoiceDB:
    @staticmethod
    def create_invoice(po_id: int, invoice_number: str, invoice_date: str, amount: float, **kwargs) -> int:
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT INTO invoices (po_id, invoice_number, invoice_date, amount, payment_status, created_date)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (po_id, invoice_number, invoice_date, amount, kwargs.get('payment_status', 'received'),
                   datetime.now().isoformat()))
        invoice_id = c.lastrowid
        total_invoiced = pd.read_sql(f"SELECT SUM(amount) as total FROM invoices WHERE po_id={po_id}", conn).iloc[0]['total']
        c.execute("UPDATE purchase_orders SET invoiced_to_date=? WHERE id=?", (total_invoiced, po_id))
        conn.commit()
        conn.close()
        return invoice_id
    
    @staticmethod
    def get_invoices(po_id: Optional[int] = None, project_id: Optional[int] = None) -> pd.DataFrame:
        conn = get_connection()
        if po_id:
            df = pd.read_sql(f"SELECT * FROM invoices WHERE po_id={po_id}", conn)
        elif project_id:
            df = pd.read_sql(f"SELECT i.* FROM invoices i JOIN purchase_orders po ON i.po_id = po.id WHERE po.project_id={project_id}", conn)
        else:
            df = pd.read_sql("SELECT * FROM invoices", conn)
        conn.close()
        return df

class TimesheetDB:
    @staticmethod
    def import_timesheets(project_id: int, df: pd.DataFrame, batch_id: str):
        conn = get_connection()
        c = conn.cursor()
        for _, row in df.iterrows():
            c.execute('''INSERT INTO timesheets (project_id, date, staff_name, task_name, hours, function,
                         discipline, rate, cost, week_ending, import_batch_id, import_date)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (project_id, row['date'], row['staff_name'], row.get('task_name', ''), row['hours'],
                       row['function'], row.get('discipline', ''), row['rate'], row['cost'],
                       row['week_ending'], batch_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_timesheets(project_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        conn = get_connection()
        query = f"SELECT * FROM timesheets WHERE project_id={project_id}"
        if start_date:
            query += f" AND date >= '{start_date}'"
        if end_date:
            query += f" AND date <= '{end_date}'"
        query += " ORDER BY date"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    @staticmethod
    def get_weekly_summary(project_id: int) -> pd.DataFrame:
        conn = get_connection()
        df = pd.read_sql(f"""SELECT week_ending, function, discipline, SUM(hours) as hours, SUM(cost) as cost
                             FROM timesheets WHERE project_id={project_id}
                             GROUP BY week_ending, function, discipline ORDER BY week_ending""", conn)
        conn.close()
        return df

class ManningDB:
    @staticmethod
    def update_forecast(project_id: int, person_name: str, week_ending: str, forecast_hours: float, position: str, rate: float):
        conn = get_connection()
        c = conn.cursor()
        staff = pd.read_sql(f"SELECT * FROM staff WHERE name='{person_name}'", conn)
        if staff.empty:
            return
        discipline = staff.iloc[0]['discipline']
        function = staff.iloc[0]['function']
        c.execute('''INSERT OR REPLACE INTO manning_forecast (project_id, person_name, position, discipline, function,
                     week_ending, forecast_hours, hourly_rate, forecast_cost, modified_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_id, person_name, position, discipline, function, week_ending, forecast_hours,
                   rate, forecast_hours * rate, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_manning_forecast(project_id: int, start_week: Optional[str] = None) -> pd.DataFrame:
        conn = get_connection()
        query = f"SELECT * FROM manning_forecast WHERE project_id={project_id}"
        if start_week:
            query += f" AND week_ending >= '{start_week}'"
        query += " ORDER BY week_ending, person_name"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    @staticmethod
    def get_forecast_reconciliation(project_id: int) -> Dict:
        conn = get_connection()
        deliv_ftc = pd.read_sql(f"SELECT SUM(forecast_to_complete) as ftc FROM deliverables WHERE project_id={project_id}", conn).iloc[0]['ftc']
        manning_ftc = pd.read_sql(f"SELECT SUM(forecast_hours) as ftc FROM manning_forecast WHERE project_id={project_id} AND week_ending > date('now')", conn).iloc[0]['ftc']
        conn.close()
        return {'deliverable_ftc': deliv_ftc if deliv_ftc else 0,
                'manning_ftc': manning_ftc if manning_ftc else 0,
                'variance': (deliv_ftc if deliv_ftc else 0) - (manning_ftc if manning_ftc else 0)}

class SnapshotDB:
    @staticmethod
    def create_snapshot(project_id: int, week_ending: str):
        conn = get_connection()
        c = conn.cursor()
        project = ProjectDB.get_project(project_id)
        deliverables = DeliverableDB.get_deliverables(project_id)
        summary = ProjectDB.get_project_summary(project_id)
        budget = deliverables['budget_hours'].sum()
        actuals = TimesheetDB.get_timesheets(project_id)
        actual_hours = actuals['hours'].sum() if not actuals.empty else 0
        earned = DeliverableDB.calculate_earned_value(project_id)['earned_hours']
        ftc = deliverables['forecast_to_complete'].sum()
        c.execute('''INSERT INTO weekly_snapshots (project_id, snapshot_date, week_ending, project_state,
                     deliverable_state, forecast_state, budget_hours, actual_hours, earned_hours,
                     forecast_to_complete, forecast_at_completion, created_by)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_id, datetime.now().isoformat(), week_ending, json.dumps(project),
                   deliverables.to_json(), json.dumps(summary), budget, actual_hours, earned, ftc,
                   actual_hours + ftc, 'system'))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_snapshots(project_id: int) -> pd.DataFrame:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM weekly_snapshots WHERE project_id={project_id} ORDER BY snapshot_date DESC", conn)
        conn.close()
        return df

class CommentaryDB:
    @staticmethod
    def save_commentary(project_id: int, week_ending: str, **kwargs):
        conn = get_connection()
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO weekly_commentary (project_id, week_ending, key_activities,
                     next_period_activities, issues_risks, general_notes, schedule_variance_notes,
                     cost_variance_notes, forecast_change_notes, created_date)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (project_id, week_ending, kwargs.get('key_activities', ''),
                   kwargs.get('next_period_activities', ''), kwargs.get('issues_risks', ''),
                   kwargs.get('general_notes', ''), kwargs.get('schedule_variance_notes', ''),
                   kwargs.get('cost_variance_notes', ''), kwargs.get('forecast_change_notes', ''),
                   datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_commentary(project_id: int, week_ending: str) -> Optional[Dict]:
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM weekly_commentary WHERE project_id={project_id} AND week_ending='{week_ending}'", conn)
        conn.close()
        return df.iloc[0].to_dict() if not df.empty else None

class MasterDataDB:
    @staticmethod
    def get_staff() -> pd.DataFrame:
        conn = get_connection()
        df = pd.read_sql("SELECT * FROM staff WHERE active=1", conn)
        conn.close()
        return df
    
    @staticmethod
    def get_rates() -> pd.DataFrame:
        conn = get_connection()
        df = pd.read_sql("SELECT DISTINCT position, rate FROM rate_schedule WHERE end_date IS NULL OR end_date > date('now') ORDER BY position", conn)
        conn.close()
        return df
    
    @staticmethod
    def get_rate_for_position(position: str, as_of_date: Optional[str] = None) -> float:
        if not as_of_date:
            as_of_date = datetime.now().strftime('%Y-%m-%d')
        conn = get_connection()
        df = pd.read_sql(f"""SELECT rate FROM rate_schedule WHERE position='{position}'
                             AND effective_date <= '{as_of_date}'
                             AND (end_date IS NULL OR end_date > '{as_of_date}')
                             ORDER BY effective_date DESC LIMIT 1""", conn)
        conn.close()
        return df.iloc[0]['rate'] if not df.empty else 170.0

try:
    init_database()
except Exception as e:
    print(f"Init error: {e}")
