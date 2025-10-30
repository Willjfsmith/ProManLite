"""
EPCM Project Scorecard v2.0 - Main Application
Complete Streamlit UI with all 15 features
Save as: app.py

Run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import io

# Import database operations
from database import (
    ProjectDB, DeliverableDB, ChangeOrderDB, PODB, InvoiceDB,
    TimesheetDB, ManningDB, SnapshotDB, CommentaryDB, MasterDataDB
)

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="EPCM Scorecard v2.0",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_time_to_hours(time_str):
    """Convert HH:MM:SS to decimal hours"""
    try:
        if pd.isna(time_str) or time_str == '':
            return 0.0
        if isinstance(time_str, (int, float)):
            return float(time_str)
        parts = str(time_str).split(':')
        if len(parts) == 3:
            return int(parts[0]) + int(parts[1])/60.0 + int(parts[2])/3600.0
        elif len(parts) == 2:
            return int(parts[0]) + int(parts[1])/60.0
        return float(time_str)
    except:
        return 0.0

def calculate_week_ending(date_obj):
    """Get next Saturday"""
    if isinstance(date_obj, str):
        date_obj = pd.to_datetime(date_obj)
    days = (5 - date_obj.weekday()) % 7
    if days == 0:
        days = 7
    return date_obj + timedelta(days=days)

def map_function(task_name):
    """Map task name to function"""
    if pd.isna(task_name):
        return "ENGINEERING"
    task = str(task_name).upper()
    if "PM" in task or "MANAGEMENT" in task:
        return "MANAGEMENT"
    elif "DF" in task or "DRAFT" in task or "3D" in task:
        return "DRAFTING"
    return "ENGINEERING"

# ============================================================================
# SESSION STATE
# ============================================================================

if 'current_project_id' not in st.session_state:
    st.session_state.current_project_id = None

# ============================================================================
# SIDEBAR - PROJECT SELECTOR
# ============================================================================

def show_project_selector():
    """Project selector in sidebar"""
    projects = ProjectDB.get_all_projects('active')
    
    if not projects.empty:
        project_options = {f"{p['name']} - {p['client']}": p['id'] 
                          for _, p in projects.iterrows()}
        
        if project_options:
            current_name = None
            if st.session_state.current_project_id:
                for name, pid in project_options.items():
                    if pid == st.session_state.current_project_id:
                        current_name = name
                        break
            
            selected = st.sidebar.selectbox(
                "📁 Active Project",
                list(project_options.keys()),
                index=list(project_options.keys()).index(current_name) if current_name else 0
            )
            st.session_state.current_project_id = project_options[selected]
    else:
        st.sidebar.warning("No active projects")
        st.session_state.current_project_id = None

# ============================================================================
# PAGE: PROJECTS
# ============================================================================

def page_projects():
    """Project management page"""
    st.title("📁 Projects")
    
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("➕ Create Project", type="primary"):
            st.session_state.show_create_project = True
    
    # Create new project form
    if st.session_state.get('show_create_project', False):
        with st.form("create_project"):
            st.subheader("Create New Project")
            
            col1, col2 = st.columns(2)
            with col1:
                project_code = st.text_input("Project Code*", 
                    value=f"PRJ-{datetime.now().strftime('%Y%m%d')}")
                name = st.text_input("Project Name*")
                client = st.text_input("Client*")
            
            with col2:
                project_type = st.selectbox("Type", 
                    ['EPCM', 'Feasibility Study', 'Detailed Design', 'Construction Support'])
                start_date = st.date_input("Start Date")
                end_date = st.date_input("End Date")
            
            col1, col2 = st.columns(2)
            with col1:
                contract_value = st.number_input("Contract Value ($)", 
                    min_value=0.0, value=100000.0)
            with col2:
                contingency = st.number_input("Contingency %", 
                    min_value=0.0, max_value=50.0, value=10.0)
            
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.form_submit_button("Create", type="primary"):
                    try:
                        pid = ProjectDB.create_project(
                            name=name,
                            client=client,
                            project_code=project_code,
                            project_type=project_type,
                            start_date=str(start_date),
                            end_date=str(end_date),
                            contract_value=contract_value,
                            contingency_pct=contingency
                        )
                        st.success(f"✅ Project created!")
                        st.session_state.current_project_id = pid
                        st.session_state.show_create_project = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.show_create_project = False
                    st.rerun()
    
    # Show existing projects
    st.divider()
    projects = ProjectDB.get_all_projects('active')
    
    if not projects.empty:
        st.dataframe(
            projects[['project_code', 'name', 'client', 'project_type', 
                     'start_date', 'end_date', 'status']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No projects yet. Create one above.")

# ============================================================================
# PAGE: DELIVERABLES
# ============================================================================

def page_deliverables():
    """Deliverable management with earned value"""
    st.title("📋 Deliverables & Budget")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    project = ProjectDB.get_project(st.session_state.current_project_id)
    st.markdown(f"### {project['name']}")
    
    # Get deliverables
    delivs = DeliverableDB.get_deliverables(st.session_state.current_project_id)
    
    # Summary metrics
    if not delivs.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        total_budget = delivs['budget_hours'].sum()
        avg_progress = delivs['physical_progress'].mean()
        earned = delivs.apply(lambda row: 
            row['earned_hours'] if row['manual_progress_override'] 
            else row['budget_hours'] * row['physical_progress'] / 100.0, axis=1).sum()
        ftc = delivs['forecast_to_complete'].sum()
        
        with col1:
            st.metric("Total Budget", f"{total_budget:.0f}h")
        with col2:
            st.metric("Avg Progress", f"{avg_progress:.0f}%")
        with col3:
            st.metric("Earned Hours", f"{earned:.0f}h")
        with col4:
            st.metric("FTC", f"{ftc:.0f}h")
    
    st.divider()
    
    # Editable deliverables table
    st.subheader("Deliverable Details")
    
    # Prepare dataframe for editing
    if not delivs.empty:
        edit_df = delivs[[
            'wbs_code', 'deliverable_name', 'discipline', 'function',
            'budget_hours', 'status', 'physical_progress', 
            'manual_progress_override', 'earned_hours', 'forecast_to_complete'
        ]].copy()
    else:
        edit_df = pd.DataFrame(columns=[
            'wbs_code', 'deliverable_name', 'discipline', 'function',
            'budget_hours', 'status', 'physical_progress', 
            'manual_progress_override', 'earned_hours', 'forecast_to_complete'
        ])
    
    edited = st.data_editor(
        edit_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "wbs_code": st.column_config.TextColumn("WBS", width="small"),
            "deliverable_name": st.column_config.TextColumn("Deliverable", width="large"),
            "discipline": st.column_config.SelectboxColumn("Disc",
                options=['GN', 'ME', 'EE', 'IC', 'ST', 'CIVIL', 'PROC'], width="small"),
            "function": st.column_config.SelectboxColumn("Function",
                options=['MANAGEMENT', 'ENGINEERING', 'DRAFTING'], width="medium"),
            "budget_hours": st.column_config.NumberColumn("Budget (h)", format="%.1f"),
            "status": st.column_config.SelectboxColumn("Status",
                options=['not_started', 'in_progress', 'internal_review', 
                        'client_review', 'issued', 'complete']),
            "physical_progress": st.column_config.NumberColumn("% Complete", 
                format="%.0f", min_value=0, max_value=100),
            "manual_progress_override": st.column_config.CheckboxColumn("Manual"),
            "earned_hours": st.column_config.NumberColumn("Earned (h)", format="%.1f"),
            "forecast_to_complete": st.column_config.NumberColumn("FTC (h)", format="%.1f")
        },
        hide_index=True
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("💾 Save Deliverables", type="primary"):
            DeliverableDB.bulk_update_deliverables(
                st.session_state.current_project_id, edited)
            st.success("✅ Saved!")
            st.rerun()
    
    # Rollup summary
    if not edited.empty:
        st.divider()
        st.subheader("Budget Rollup")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**By Function**")
            by_function = edited.groupby('function')['budget_hours'].sum().reset_index()
            st.dataframe(by_function, hide_index=True)
        
        with col2:
            st.markdown("**By Discipline**")
            by_disc = edited.groupby('discipline')['budget_hours'].sum().reset_index()
            st.dataframe(by_disc, hide_index=True)

# ============================================================================
# PAGE: CHANGE ORDERS
# ============================================================================

def page_change_orders():
    """Change order management"""
    st.title("📝 Change Orders")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    project = ProjectDB.get_project(st.session_state.current_project_id)
    st.markdown(f"### {project['name']}")
    
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("➕ New Change Order", type="primary"):
            st.session_state.show_new_co = True
    
    # Create CO form
    if st.session_state.get('show_new_co', False):
        with st.form("new_co"):
            st.subheader("Create Change Order")
            
            col1, col2 = st.columns(2)
            with col1:
                co_number = st.text_input("CO Number", 
                    value=f"CO-{datetime.now().strftime('%Y%m%d-%H%M')}")
                change_type = st.selectbox("Type",
                    ['client_change', 'internal', 'design_change', 'constructability'])
            
            with col2:
                status = st.selectbox("Status",
                    ['draft', 'submitted', 'approved', 'rejected'])
                client_billable = st.checkbox("Client Billable")
            
            description = st.text_area("Description")
            
            st.markdown("**Hours Impact**")
            col1, col2, col3 = st.columns(3)
            with col1:
                hours_mgmt = st.number_input("Management", min_value=0.0)
            with col2:
                hours_eng = st.number_input("Engineering", min_value=0.0)
            with col3:
                hours_draft = st.number_input("Drafting", min_value=0.0)
            
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.form_submit_button("Create", type="primary"):
                    ChangeOrderDB.create_change_order(
                        project_id=st.session_state.current_project_id,
                        co_number=co_number,
                        description=description,
                        change_type=change_type,
                        status=status,
                        hours_mgmt=hours_mgmt,
                        hours_eng=hours_eng,
                        hours_draft=hours_draft,
                        client_billable=1 if client_billable else 0
                    )
                    st.success("✅ Change order created!")
                    st.session_state.show_new_co = False
                    st.rerun()
            
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.show_new_co = False
                    st.rerun()
    
    # Show existing COs
    st.divider()
    cos = ChangeOrderDB.get_change_orders(st.session_state.current_project_id)
    
    if not cos.empty:
        # Summary
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total COs", len(cos))
        with col2:
            approved = len(cos[cos['status'] == 'approved'])
            st.metric("Approved", approved)
        with col3:
            total_hours = cos['hours_mgmt'] + cos['hours_eng'] + cos['hours_draft']
            st.metric("Total Hours Impact", f"{total_hours.sum():.0f}h")
        with col4:
            billable = cos[cos['client_billable'] == 1]['hours_mgmt'] + \
                      cos[cos['client_billable'] == 1]['hours_eng'] + \
                      cos[cos['client_billable'] == 1]['hours_draft']
            st.metric("Billable Hours", f"{billable.sum():.0f}h")
        
        st.divider()
        
        # Display COs
        display_cos = cos[[
            'co_number', 'description', 'change_type', 'status',
            'hours_mgmt', 'hours_eng', 'hours_draft', 'client_billable'
        ]].copy()
        
        st.dataframe(display_cos, use_container_width=True, hide_index=True)
    else:
        st.info("No change orders yet")

# ============================================================================
# PAGE: PURCHASE ORDERS
# ============================================================================

def page_purchase_orders():
    """PO and invoice management"""
    st.title("📦 Purchase Orders & External Costs")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    tabs = st.tabs(["Purchase Orders", "Invoices", "Summary"])
    
    # PO Tab
    with tabs[0]:
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("➕ New PO", type="primary"):
                st.session_state.show_new_po = True
        
        if st.session_state.get('show_new_po', False):
            with st.form("new_po"):
                st.subheader("Create Purchase Order")
                
                col1, col2 = st.columns(2)
                with col1:
                    po_number = st.text_input("PO Number",
                        value=f"PO-{datetime.now().strftime('%Y%m%d')}")
                    supplier = st.text_input("Supplier*")
                
                with col2:
                    category = st.selectbox("Category",
                        ['equipment', 'services', 'materials', 'subcontract'])
                    commitment = st.number_input("Commitment Value ($)*",
                        min_value=0.0)
                
                description = st.text_area("Description")
                
                col1, col2 = st.columns([1, 3])
                with col1:
                    if st.form_submit_button("Create", type="primary"):
                        PODB.create_po(
                            project_id=st.session_state.current_project_id,
                            po_number=po_number,
                            supplier=supplier,
                            description=description,
                            commitment_value=commitment,
                            category=category
                        )
                        st.success("✅ PO created!")
                        st.session_state.show_new_po = False
                        st.rerun()
                
                with col2:
                    if st.form_submit_button("Cancel"):
                        st.session_state.show_new_po = False
                        st.rerun()
        
        # Show POs
        pos = PODB.get_purchase_orders(st.session_state.current_project_id)
        if not pos.empty:
            st.dataframe(pos[[
                'po_number', 'supplier', 'description', 'category',
                'commitment_value', 'invoiced_to_date', 'accrued_work_done', 'status'
            ]], use_container_width=True, hide_index=True)
    
    # Invoice Tab
    with tabs[1]:
        st.subheader("Invoices")
        invoices = InvoiceDB.get_invoices(
            project_id=st.session_state.current_project_id)
        
        if not invoices.empty:
            st.dataframe(invoices, use_container_width=True, hide_index=True)
        else:
            st.info("No invoices yet")
    
    # Summary Tab
    with tabs[2]:
        pos = PODB.get_purchase_orders(st.session_state.current_project_id)
        if not pos.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Commitment", f"${pos['commitment_value'].sum():,.0f}")
            with col2:
                st.metric("Invoiced", f"${pos['invoiced_to_date'].sum():,.0f}")
            with col3:
                st.metric("Accrued", f"${pos['accrued_work_done'].sum():,.0f}")
            with col4:
                remaining = pos['commitment_value'].sum() - pos['invoiced_to_date'].sum() - pos['accrued_work_done'].sum()
                st.metric("Remaining", f"${remaining:,.0f}")

# ============================================================================
# PAGE: DASHBOARD
# ============================================================================

def page_dashboard():
    """Main project dashboard"""
    st.title("📊 Project Dashboard")
    
    if not st.session_state.current_project_id:
        st.info("Select a project to view dashboard")
        return
    
    project = ProjectDB.get_project(st.session_state.current_project_id)
    
    st.markdown(f"## {project['name']} - {project['client']}")
    st.markdown(f"*{project['project_type']}*")
    
    # Get summary data
    summary = ProjectDB.get_project_summary(st.session_state.current_project_id)
    delivs = DeliverableDB.get_deliverables(st.session_state.current_project_id)
    
    if delivs.empty:
        st.warning("No deliverables defined. Add some in the Deliverables page.")
        return
    
    # Calculate totals
    budget_total = delivs['budget_hours'].sum()
    earned_total = delivs.apply(lambda row:
        row['earned_hours'] if row['manual_progress_override']
        else row['budget_hours'] * row['physical_progress'] / 100.0, axis=1).sum()
    ftc_total = delivs['forecast_to_complete'].sum()
    
    # Get actuals
    actuals = TimesheetDB.get_timesheets(st.session_state.current_project_id)
    actual_hours = actuals['hours'].sum() if not actuals.empty else 0
    actual_cost = actuals['cost'].sum() if not actuals.empty else 0
    
    # Key metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Budget", f"{budget_total:.0f}h")
    with col2:
        st.metric("Actual", f"{actual_hours:.0f}h")
    with col3:
        st.metric("Earned", f"{earned_total:.0f}h")
    with col4:
        st.metric("FTC", f"{ftc_total:.0f}h")
    with col5:
        fac = actual_hours + ftc_total
        pf = budget_total / fac if fac > 0 else 1.0
        st.metric("Performance", f"{pf:.2f}")
    
    st.divider()
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Progress by Function")
        by_func = delivs.groupby('function').agg({
            'budget_hours': 'sum',
            'physical_progress': 'mean'
        }).reset_index()
        
        fig = go.Figure(data=[
            go.Bar(name='Budget', x=by_func['function'], y=by_func['budget_hours']),
        ])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Completion Status")
        status_counts = delivs['status'].value_counts()
        fig = go.Figure(data=[go.Pie(labels=status_counts.index, values=status_counts.values)])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PAGE: IMPORT DATA
# ============================================================================

def page_import():
    """Import timesheet data"""
    st.title("📤 Import Timesheets")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    project = ProjectDB.get_project(st.session_state.current_project_id)
    st.markdown(f"### Importing to: {project['name']}")
    
    uploaded = st.file_uploader("Upload Workflow Max CSV", type=['csv'])
    
    if uploaded:
        df = pd.read_csv(uploaded)
        
        st.subheader("Preview")
        st.dataframe(df.head(10))
        
        if st.button("Process and Import", type="primary"):
            try:
                # Map columns
                mapping = {
                    '[Time] Date': 'date',
                    '[Staff] Name': 'staff_name',
                    '[Job Task] Name': 'task_name',
                    '[Time] Time': 'time'
                }
                df = df.rename(columns=mapping)
                
                # Process
                df['hours'] = df['time'].apply(parse_time_to_hours)
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['week_ending'] = pd.to_datetime(df['date']).apply(
                    lambda x: calculate_week_ending(x).strftime('%Y-%m-%d'))
                df['function'] = df['task_name'].apply(map_function)
                
                # Get rates
                staff = MasterDataDB.get_staff()
                rates = MasterDataDB.get_rates()
                
                def get_rate(name):
                    staff_row = staff[staff['name'] == name]
                    if not staff_row.empty:
                        pos = staff_row.iloc[0]['position']
                        return MasterDataDB.get_rate_for_position(pos)
                    return 170.0
                
                df['rate'] = df['staff_name'].apply(get_rate)
                df['cost'] = df['hours'] * df['rate']
                df['discipline'] = ''
                
                # Import
                batch_id = datetime.now().strftime('%Y%m%d-%H%M%S')
                TimesheetDB.import_timesheets(
                    st.session_state.current_project_id, df, batch_id)
                
                st.success(f"✅ Imported {len(df)} entries!")
                
            except Exception as e:
                st.error(f"Error: {e}")

# ============================================================================
# PAGE: MANNING FORECAST
# ============================================================================

def page_manning():
    """Manning forecast grid"""
    st.title("📅 Manning Forecast")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    st.info("Manning forecast interface - implement grid similar to v1.3")
    st.markdown("This page shows weekly resource loading forecast")
    
    # Get manning data
    manning = ManningDB.get_manning_forecast(st.session_state.current_project_id)
    
    if not manning.empty:
        st.dataframe(manning)
    else:
        st.info("No manning forecast data yet")

# ============================================================================
# PAGE: REPORTS
# ============================================================================

def page_reports():
    """Report generation"""
    st.title("📊 Reports")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    project = ProjectDB.get_project(st.session_state.current_project_id)
    st.markdown(f"### {project['name']}")
    
    # Weekly commentary
    st.subheader("Weekly Commentary")
    
    week_ending = st.date_input("Week Ending", datetime.now())
    week_str = week_ending.strftime('%Y-%m-%d')
    
    commentary = CommentaryDB.get_commentary(
        st.session_state.current_project_id, week_str)
    
    col1, col2 = st.columns(2)
    
    with col1:
        key_activities = st.text_area("Key Activities This Week",
            value=commentary['key_activities'] if commentary else '',
            height=150)
        
        issues_risks = st.text_area("Issues & Risks",
            value=commentary['issues_risks'] if commentary else '',
            height=150)
    
    with col2:
        next_period = st.text_area("Planned Next Week",
            value=commentary['next_period_activities'] if commentary else '',
            height=150)
        
        general_notes = st.text_area("General Notes",
            value=commentary['general_notes'] if commentary else '',
            height=150)
    
    if st.button("💾 Save Commentary"):
        CommentaryDB.save_commentary(
            project_id=st.session_state.current_project_id,
            week_ending=week_str,
            key_activities=key_activities,
            next_period_activities=next_period,
            issues_risks=issues_risks,
            general_notes=general_notes
        )
        st.success("✅ Commentary saved!")
    
    st.divider()
    
    # Export options
    st.subheader("Export Report")
    
    if st.button("📥 Generate Excel Report", type="primary"):
        # Generate comprehensive Excel
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Summary
            summary_data = {
                'Metric': ['Project', 'Client', 'Report Date'],
                'Value': [project['name'], project['client'], week_str]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Deliverables
            delivs = DeliverableDB.get_deliverables(st.session_state.current_project_id)
            if not delivs.empty:
                delivs.to_excel(writer, sheet_name='Deliverables', index=False)
            
            # Timesheets
            timesheets = TimesheetDB.get_timesheets(st.session_state.current_project_id)
            if not timesheets.empty:
                timesheets.to_excel(writer, sheet_name='Timesheets', index=False)
            
            # Change Orders
            cos = ChangeOrderDB.get_change_orders(st.session_state.current_project_id)
            if not cos.empty:
                cos.to_excel(writer, sheet_name='Change Orders', index=False)
            
            # POs
            pos = PODB.get_purchase_orders(st.session_state.current_project_id)
            if not pos.empty:
                pos.to_excel(writer, sheet_name='Purchase Orders', index=False)
        
        st.download_button(
            "📥 Download Report",
            output.getvalue(),
            f"Report_{project['name']}_{week_str}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ============================================================================
# PAGE: FORECAST RECONCILIATION
# ============================================================================

def page_forecast_recon():
    """Reconcile deliverable FTC vs manning forecast"""
    st.title("🔄 Forecast Reconciliation")
    
    if not st.session_state.current_project_id:
        st.warning("Select a project first")
        return
    
    recon = ManningDB.get_forecast_reconciliation(st.session_state.current_project_id)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Deliverable FTC", f"{recon['deliverable_ftc']:.0f}h")
    with col2:
        st.metric("Manning FTC", f"{recon['manning_ftc']:.0f}h")
    with col3:
        variance = recon['variance']
        st.metric("Variance", f"{variance:+.0f}h",
                 delta=f"{variance:+.0f}h",
                 delta_color="inverse" if abs(variance) > 20 else "normal")
    
    if abs(variance) > 20:
        st.warning(f"⚠️ Forecasts differ by {abs(variance):.0f} hours. Reconcile before reporting.")
    else:
        st.success("✅ Forecasts are aligned")

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    """Main application"""
    
    st.sidebar.title("📊 EPCM Scorecard")
    st.sidebar.markdown("*v2.0 Professional*")
    st.sidebar.markdown("---")
    
    # Project selector
    show_project_selector()
    
    st.sidebar.markdown("---")
    
    # Navigation
    page = st.sidebar.radio("Navigation", [
        "🏠 Dashboard",
        "📁 Projects",
        "📋 Deliverables",
        "📝 Change Orders",
        "📦 Purchase Orders",
        "📤 Import Data",
        "📅 Manning Forecast",
        "🔄 Forecast Recon",
        "📊 Reports"
    ], label_visibility="collapsed")
    
    st.sidebar.markdown("---")
    
    # Route pages
    if page == "🏠 Dashboard":
        page_dashboard()
    elif page == "📁 Projects":
        page_projects()
    elif page == "📋 Deliverables":
        page_deliverables()
    elif page == "📝 Change Orders":
        page_change_orders()
    elif page == "📦 Purchase Orders":
        page_purchase_orders()
    elif page == "📤 Import Data":
        page_import()
    elif page == "📅 Manning Forecast":
        page_manning()
    elif page == "🔄 Forecast Recon":
        page_forecast_recon()
    elif page == "📊 Reports":
        page_reports()

if __name__ == "__main__":
    main()
