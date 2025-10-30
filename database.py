"""
EPCM Project Scorecard v2.0 - Database Operations Module
Complete database interface with all CRUD operations
Save as: database.py
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
from typing import Optional, List, Dict, Tuple

DB_NAME = 'scorecard_v2.db'

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def init_database():
    """Initialize database with full schema"""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Execute full schema from schema file
    with open('database_schema_v2.sql', 'r') as f:
        schema_sql = f.read()
        # Remove comments and split into statements
        statements = [s.strip() for s in schema_sql.split(';') if s.strip() and not s.strip().startswith('#')]
        for statement in statements:
            try:
                c.execute(statement)
            except:
                pass  # Skip if already exists
    
    conn.commit()
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
        """Get all projects"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM projects WHERE status='{status}' ORDER BY created_date DESC", conn)
        conn.close()
        return df
    
    @staticmethod
    def update_project(project_id: int, **kwargs):
        """Update project fields"""
        conn = get_connection()
        c = conn.cursor()
        
        # Build update statement dynamically
        fields = ', '.join([f"{k}=?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [project_id]
        
        c.execute(f"UPDATE projects SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_project_summary(project_id: int) -> Dict:
        """Get comprehensive project summary"""
        conn = get_connection()
        
        # Budget from deliverables
        budget = pd.read_sql(f"""
            SELECT function, SUM(budget_hours) as budget_hours
            FROM deliverables WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        # Actuals from timesheets
        actuals = pd.read_sql(f"""
            SELECT function, SUM(hours) as actual_hours, SUM(cost) as actual_cost
            FROM timesheets WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        # Earned from deliverables
        earned = pd.read_sql(f"""
            SELECT function,
            SUM(CASE WHEN manual_progress_override=1 THEN earned_hours
                     ELSE budget_hours * physical_progress / 100.0 END) as earned_hours
            FROM deliverables WHERE project_id={project_id}
            GROUP BY function
        """, conn)
        
        # Forecast
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
        
        # Delete existing
        c.execute(f"DELETE FROM deliverables WHERE project_id={project_id}")
        
        # Insert new
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

# ============================================================================
# CHANGE ORDER OPERATIONS
# ============================================================================

class ChangeOrderDB:
    """Change order database operations"""
    
    @staticmethod
    def create_change_order(project_id: int, co_number: str, description: str,
                           change_type: str, **kwargs) -> int:
        """Create change order"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute('''INSERT INTO change_orders 
            (project_id, co_number, description, change_type, status,
             hours_mgmt, hours_eng, hours_draft, client_billable,
             estimated_cost, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_id, co_number, description, change_type,
             kwargs.get('status', 'draft'),
             kwargs.get('hours_mgmt', 0),
             kwargs.get('hours_eng', 0),
             kwargs.get('hours_draft', 0),
             kwargs.get('client_billable', 0),
             kwargs.get('estimated_cost', 0),
             datetime.now().isoformat()))
        
        co_id = c.lastrowid
        conn.commit()
        conn.close()
        return co_id
    
    @staticmethod
    def get_change_orders(project_id: int) -> pd.DataFrame:
        """Get all change orders"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM change_orders WHERE project_id={project_id} ORDER BY created_date DESC", conn)
        conn.close()
        return df
    
    @staticmethod
    def update_change_order(co_id: int, **kwargs):
        """Update change order"""
        conn = get_connection()
        c = conn.cursor()
        
        fields = ', '.join([f"{k}=?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [co_id]
        
        c.execute(f"UPDATE change_orders SET {fields} WHERE id=?", values)
        conn.commit()
        conn.close()
    
    @staticmethod
    def incorporate_change_order(co_id: int, target_deliverables: List[int]):
        """Incorporate approved CO into project budget"""
        conn = get_connection()
        c = conn.cursor()
        
        # Get CO details
        co = pd.read_sql(f"SELECT * FROM change_orders WHERE id={co_id}", conn).iloc[0]
        
        # Update CO status
        c.execute("UPDATE change_orders SET status='incorporated', incorporated_date=? WHERE id=?",
                 (datetime.now().isoformat(), co_id))
        
        # Add hours to deliverables (distribute proportionally)
        total_hours = co['hours_mgmt'] + co['hours_eng'] + co['hours_draft']
        for deliv_id in target_deliverables:
            c.execute("UPDATE deliverables SET budget_hours=budget_hours+?, forecast_to_complete=forecast_to_complete+? WHERE id=?",
                     (total_hours / len(target_deliverables), 
                      total_hours / len(target_deliverables),
                      deliv_id))
        
        conn.commit()
        conn.close()

# ============================================================================
# PURCHASE ORDER & INVOICE OPERATIONS
# ============================================================================

class PODB:
    """Purchase order operations"""
    
    @staticmethod
    def create_po(project_id: int, po_number: str, supplier: str,
                  description: str, commitment_value: float, **kwargs) -> int:
        """Create purchase order"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute('''INSERT INTO purchase_orders 
            (project_id, po_number, supplier, description, category,
             commitment_value, status, issue_date, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_id, po_number, supplier, description,
             kwargs.get('category', 'services'),
             commitment_value, 'issued',
             kwargs.get('issue_date', datetime.now().strftime('%Y-%m-%d')),
             datetime.now().isoformat()))
        
        po_id = c.lastrowid
        conn.commit()
        conn.close()
        return po_id
    
    @staticmethod
    def get_purchase_orders(project_id: int) -> pd.DataFrame:
        """Get all POs"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM purchase_orders WHERE project_id={project_id}", conn)
        conn.close()
        return df
    
    @staticmethod
    def update_po_accrual(po_id: int, accrued_work_done: float):
        """Update accrual for work done"""
        conn = get_connection()
        c = conn.cursor()
        
        # Get current invoiced amount
        invoiced = pd.read_sql(f"SELECT SUM(amount) as total FROM invoices WHERE po_id={po_id}", conn).iloc[0]['total']
        invoiced = invoiced if invoiced else 0
        
        # Get commitment
        commitment = pd.read_sql(f"SELECT commitment_value FROM purchase_orders WHERE id={po_id}", conn).iloc[0]['commitment_value']
        
        # Calculate remaining
        remaining = commitment - invoiced - accrued_work_done
        
        c.execute('''UPDATE purchase_orders 
                    SET accrued_work_done=?, remaining_commitment=?
                    WHERE id=?''',
                 (accrued_work_done, remaining, po_id))
        
        conn.commit()
        conn.close()

class InvoiceDB:
    """Invoice operations"""
    
    @staticmethod
    def create_invoice(po_id: int, invoice_number: str, invoice_date: str,
                      amount: float, **kwargs) -> int:
        """Create invoice"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute('''INSERT INTO invoices 
            (po_id, invoice_number, invoice_date, amount,
             payment_status, created_date)
            VALUES (?, ?, ?, ?, ?, ?)''',
            (po_id, invoice_number, invoice_date, amount,
             kwargs.get('payment_status', 'received'),
             datetime.now().isoformat()))
        
        invoice_id = c.lastrowid
        
        # Update PO invoiced total
        total_invoiced = pd.read_sql(f"SELECT SUM(amount) as total FROM invoices WHERE po_id={po_id}", conn).iloc[0]['total']
        c.execute("UPDATE purchase_orders SET invoiced_to_date=? WHERE id=?", (total_invoiced, po_id))
        
        conn.commit()
        conn.close()
        return invoice_id
    
    @staticmethod
    def get_invoices(po_id: Optional[int] = None, project_id: Optional[int] = None) -> pd.DataFrame:
        """Get invoices"""
        conn = get_connection()
        
        if po_id:
            df = pd.read_sql(f"SELECT * FROM invoices WHERE po_id={po_id}", conn)
        elif project_id:
            df = pd.read_sql(f"""
                SELECT i.* FROM invoices i
                JOIN purchase_orders po ON i.po_id = po.id
                WHERE po.project_id={project_id}
            """, conn)
        else:
            df = pd.read_sql("SELECT * FROM invoices", conn)
        
        conn.close()
        return df

# ============================================================================
# TIMESHEET OPERATIONS
# ============================================================================

class TimesheetDB:
    """Timesheet operations"""
    
    @staticmethod
    def import_timesheets(project_id: int, df: pd.DataFrame, batch_id: str):
        """Bulk import timesheets"""
        conn = get_connection()
        c = conn.cursor()
        
        for _, row in df.iterrows():
            c.execute('''INSERT INTO timesheets 
                (project_id, date, staff_name, task_name, hours, function,
                 discipline, rate, cost, week_ending, import_batch_id, import_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (project_id, row['date'], row['staff_name'], row.get('task_name', ''),
                 row['hours'], row['function'], row.get('discipline', ''),
                 row['rate'], row['cost'], row['week_ending'],
                 batch_id, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_timesheets(project_id: int, start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """Get timesheets with optional date filter"""
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
        """Get weekly spend summary"""
        conn = get_connection()
        df = pd.read_sql(f"""
            SELECT week_ending, function, discipline,
                   SUM(hours) as hours, SUM(cost) as cost
            FROM timesheets
            WHERE project_id={project_id}
            GROUP BY week_ending, function, discipline
            ORDER BY week_ending
        """, conn)
        conn.close()
        return df

# ============================================================================
# MANNING FORECAST OPERATIONS
# ============================================================================

class ManningDB:
    """Manning forecast operations"""
    
    @staticmethod
    def update_forecast(project_id: int, person_name: str, week_ending: str,
                       forecast_hours: float, position: str, rate: float):
        """Update manning forecast"""
        conn = get_connection()
        c = conn.cursor()
        
        # Get person details
        staff = pd.read_sql(f"SELECT * FROM staff WHERE name='{person_name}'", conn)
        if staff.empty:
            return
        
        discipline = staff.iloc[0]['discipline']
        function = staff.iloc[0]['function']
        
        c.execute('''INSERT OR REPLACE INTO manning_forecast 
            (project_id, person_name, position, discipline, function,
             week_ending, forecast_hours, hourly_rate, forecast_cost, modified_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_id, person_name, position, discipline, function,
             week_ending, forecast_hours, rate, forecast_hours * rate,
             datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_manning_forecast(project_id: int, start_week: Optional[str] = None) -> pd.DataFrame:
        """Get manning forecast"""
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
        """Compare deliverable FTC vs manning forecast"""
        conn = get_connection()
        
        # Deliverable FTC
        deliv_ftc = pd.read_sql(f"""
            SELECT SUM(forecast_to_complete) as ftc
            FROM deliverables WHERE project_id={project_id}
        """, conn).iloc[0]['ftc']
        
        # Manning FTC (future weeks only)
        manning_ftc = pd.read_sql(f"""
            SELECT SUM(forecast_hours) as ftc
            FROM manning_forecast 
            WHERE project_id={project_id}
            AND week_ending > date('now')
        """, conn).iloc[0]['ftc']
        
        conn.close()
        
        return {
            'deliverable_ftc': deliv_ftc if deliv_ftc else 0,
            'manning_ftc': manning_ftc if manning_ftc else 0,
            'variance': (deliv_ftc if deliv_ftc else 0) - (manning_ftc if manning_ftc else 0)
        }

# ============================================================================
# SNAPSHOT OPERATIONS
# ============================================================================

class SnapshotDB:
    """Historical snapshot operations"""
    
    @staticmethod
    def create_snapshot(project_id: int, week_ending: str):
        """Create weekly snapshot"""
        conn = get_connection()
        c = conn.cursor()
        
        # Get project state
        project = ProjectDB.get_project(project_id)
        deliverables = DeliverableDB.get_deliverables(project_id)
        summary = ProjectDB.get_project_summary(project_id)
        
        # Calculate key metrics
        budget = deliverables['budget_hours'].sum()
        actuals = TimesheetDB.get_timesheets(project_id)
        actual_hours = actuals['hours'].sum() if not actuals.empty else 0
        earned = DeliverableDB.calculate_earned_value(project_id)['earned_hours']
        ftc = deliverables['forecast_to_complete'].sum()
        
        c.execute('''INSERT INTO weekly_snapshots 
            (project_id, snapshot_date, week_ending, project_state,
             deliverable_state, forecast_state, budget_hours, actual_hours,
             earned_hours, forecast_to_complete, forecast_at_completion, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_id, datetime.now().isoformat(), week_ending,
             json.dumps(project), deliverables.to_json(), json.dumps(summary),
             budget, actual_hours, earned, ftc, actual_hours + ftc,
             'system'))
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_snapshots(project_id: int) -> pd.DataFrame:
        """Get all snapshots"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM weekly_snapshots WHERE project_id={project_id} ORDER BY snapshot_date DESC", conn)
        conn.close()
        return df

# ============================================================================
# COMMENTARY OPERATIONS
# ============================================================================

class CommentaryDB:
    """Weekly commentary operations"""
    
    @staticmethod
    def save_commentary(project_id: int, week_ending: str, **kwargs):
        """Save weekly commentary"""
        conn = get_connection()
        c = conn.cursor()
        
        c.execute('''INSERT OR REPLACE INTO weekly_commentary 
            (project_id, week_ending, key_activities, next_period_activities,
             issues_risks, general_notes, schedule_variance_notes,
             cost_variance_notes, forecast_change_notes, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (project_id, week_ending,
             kwargs.get('key_activities', ''),
             kwargs.get('next_period_activities', ''),
             kwargs.get('issues_risks', ''),
             kwargs.get('general_notes', ''),
             kwargs.get('schedule_variance_notes', ''),
             kwargs.get('cost_variance_notes', ''),
             kwargs.get('forecast_change_notes', ''),
             datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_commentary(project_id: int, week_ending: str) -> Optional[Dict]:
        """Get commentary for week"""
        conn = get_connection()
        df = pd.read_sql(f"SELECT * FROM weekly_commentary WHERE project_id={project_id} AND week_ending='{week_ending}'", conn)
        conn.close()
        return df.iloc[0].to_dict() if not df.empty else None

# ============================================================================
# MASTER DATA OPERATIONS
# ============================================================================

class MasterDataDB:
    """Master data operations"""
    
    @staticmethod
    def get_staff() -> pd.DataFrame:
        """Get all staff"""
        conn = get_connection()
        df = pd.read_sql("SELECT * FROM staff WHERE active=1", conn)
        conn.close()
        return df
    
    @staticmethod
    def get_rates() -> pd.DataFrame:
        """Get current rate schedule"""
        conn = get_connection()
        df = pd.read_sql("""
            SELECT DISTINCT position, rate FROM rate_schedule
            WHERE end_date IS NULL OR end_date > date('now')
            ORDER BY position
        """, conn)
        conn.close()
        return df
    
    @staticmethod
    def get_rate_for_position(position: str, as_of_date: Optional[str] = None) -> float:
        """Get rate for position at date"""
        if not as_of_date:
            as_of_date = datetime.now().strftime('%Y-%m-%d')
        
        conn = get_connection()
        df = pd.read_sql(f"""
            SELECT rate FROM rate_schedule
            WHERE position='{position}'
            AND effective_date <= '{as_of_date}'
            AND (end_date IS NULL OR end_date > '{as_of_date}')
            ORDER BY effective_date DESC LIMIT 1
        """, conn)
        conn.close()
        
        return df.iloc[0]['rate'] if not df.empty else 170.0

# Initialize database on module import
try:
    init_database()
except:
    pass
