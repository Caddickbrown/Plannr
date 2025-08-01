import sys
import os
import time
from datetime import datetime
import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter
import pymssql
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psutil
import gc
from io import BytesIO

# Qt6 imports
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QPushButton, QCheckBox, QTextEdit, QScrollArea, 
    QFrame, QGroupBox, QFileDialog, QMessageBox, QProgressBar,
    QSplitter, QSizePolicy
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QPalette, QColor

# Load environment variables
load_dotenv('db_credentials.env')

# VERSION INFO
VERSION = "v2.0.0"
VERSION_DATE = "2025-07-29"
DEBUG_MODE = False
DEBUG_COMPONENT_PART = None  # Set to a specific part number (as string) to track, e.g. "8034855"
DEBUG_SO_NUMBER = 9682591  # Set to a specific SO number (as string) to track, e.g. "9678417"

# Global variables
quick_analysis_excel_buffer = None

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'myuser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'mypassword')
DB_NAME = os.getenv('DB_NAME', 'mydatabase')
DB_PORT = os.getenv('DB_PORT', '1433')

# Print debug configuration at startup
if DEBUG_MODE:
    print("\n" + "="*60)
    print("🔍 DEBUG MODE ACTIVATED")
    print("="*60)
    if DEBUG_COMPONENT_PART is None and DEBUG_SO_NUMBER is None:
        print("⚠️  NO DEBUG FILTERS SET")
        print("   Set DEBUG_COMPONENT_PART or DEBUG_SO_NUMBER to see detailed output")
        print("   Example: DEBUG_COMPONENT_PART = '8039831' or DEBUG_SO_NUMBER = '9678487'")
        print("   Current settings will show general processing info only")
    else:
        print("🎯 DEBUG FILTERS ACTIVE:")
        print(f"   Component Part: {DEBUG_COMPONENT_PART if DEBUG_COMPONENT_PART else 'Not specified'}")
        print(f"   SO Number: {DEBUG_SO_NUMBER if DEBUG_SO_NUMBER else 'Not specified'}")
    print("="*60)
    print("Debug output will appear in the terminal/console during processing")
    print("="*60 + "\n")
    
    # Show data source information
    print("📊 DATA SOURCES AND COLUMN MAPPINGS:")
    print("   📦 IPIS Table (Stock):")
    print("     - Column: 'PART_NO' → Used for stock lookup")
    print("     - Column: 'Available Qty' → Stock quantity")
    print("     - Logic: Grouped by PART_NO, sum of Available Qty")
    print("")
    print("   🔒 Component Demand Table (Committed):")
    print("     - Column: 'Component Part Number' → Component identifier")
    print("     - Column: 'Component Qty Required' → Committed quantity")
    print("     - Logic: Grouped by Component Part Number, sum of Component Qty Required")
    print("")
    print("   📋 Planned Demand Table (BOM):")
    print("     - Column: 'SO Number' → Shop Order identifier")
    print("     - Column: 'Component Part Number' → Component identifier")
    print("     - Column: 'Component Qty Required' → Required quantity")
    print("     - Logic: Filtered by SO Number to get BOM components")
    print("")
    print("   📄 POs Table (Purchase Orders):")
    print("     - Column: 'Part Number' → Part identifier")
    print("     - Column: 'Qty Due' → Quantity on order")
    print("     - Column: 'Promised Due Date' → Expected delivery")
    print("     - Logic: Filtered by Part Number and future dates")
    print("")
    print("   ⏱️ Hours Table (Labor Standards):")
    print("     - Column: 'PART_NO' → Part identifier")
    print("     - Column: 'Hours per Unit' → Labor hours")
    print("     - Logic: Grouped by PART_NO, sum of Hours per Unit")
    print("")
    print("   📋 Demand Table (Main Orders):")
    print("     - Column: 'SO No' → Shop Order identifier")
    print("     - Column: 'Part No' → Parent part identifier")
    print("     - Column: 'Rev Qty Due' → Order quantity")
    print("     - Column: 'Start Date' → Order start date")
    print("     - Column: 'Planner' → Planner code")
    print("="*60 + "\n") 

# Performance tracking utilities
class PerformanceTracker:
    """Track performance metrics across different phases"""
    
    def __init__(self):
        self.phases = {}
        self.current_phase = None
        self.phase_start_time = None
        self.memory_usage = []
        self.process = psutil.Process()
    
    def start_phase(self, phase_name):
        """Start timing a new phase"""
        if self.current_phase:
            self.end_phase()
        
        self.current_phase = phase_name
        self.phase_start_time = time.time()
        memory_info = self.process.memory_info()
        self.memory_usage.append({
            'phase': phase_name,
            'memory_mb': memory_info.rss / 1024 / 1024,
            'timestamp': time.time()
        })
    
    def end_phase(self):
        """End the current phase and record timing"""
        if self.current_phase and self.phase_start_time:
            duration = time.time() - self.phase_start_time
            if self.current_phase not in self.phases:
                self.phases[self.current_phase] = []
            self.phases[self.current_phase].append(duration)
            
            # Record final memory usage for this phase
            memory_info = self.process.memory_info()
            self.memory_usage.append({
                'phase': f"{self.current_phase}_end",
                'memory_mb': memory_info.rss / 1024 / 1024,
                'timestamp': time.time()
            })
            
            self.current_phase = None
            self.phase_start_time = None
    
    def get_phase_summary(self):
        """Get summary of all phases"""
        summary = {}
        for phase, times in self.phases.items():
            if times:
                summary[phase] = {
                    'total_time': sum(times),
                    'avg_time': sum(times) / len(times),
                    'count': len(times),
                    'min_time': min(times),
                    'max_time': max(times)
                }
        return summary
    
    def get_memory_summary(self):
        """Get memory usage summary"""
        if not self.memory_usage:
            return {}
        
        memory_values = [m['memory_mb'] for m in self.memory_usage]
        return {
            'peak_memory_mb': max(memory_values),
            'avg_memory_mb': sum(memory_values) / len(memory_values),
            'initial_memory_mb': self.memory_usage[0]['memory_mb'] if self.memory_usage else 0,
            'final_memory_mb': self.memory_usage[-1]['memory_mb'] if self.memory_usage else 0
        }
    
    def cleanup(self):
        """Clean up and end any current phase"""
        if self.current_phase:
            self.end_phase()

# Global performance tracker
performance_tracker = PerformanceTracker()

def timing_decorator(phase_name):
    """Decorator to automatically time function execution"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            performance_tracker.start_phase(phase_name)
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                performance_tracker.end_phase()
        return wrapper
    return decorator

def get_database_connection():
    """Create and return a SQLAlchemy engine for SQL Server"""
    try:
        # Create connection string for SQL Server
        connection_string = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
        
        # Create SQLAlchemy engine with SSMS-like parameters
        engine = create_engine(
            connection_string,
            connect_args={
                'appname': 'Microsoft SQL Server Management Studio - Query',
                'charset': 'utf8',
                'login_timeout': 30,
                'timeout': 30
            },
            echo=False  # Set to True for SQL query logging
        )
        return engine
    except Exception as e:
        raise Exception(f"SQL Server connection failed: {str(e)}")

def execute_query(query, params=None):
    """Execute a SQL query and return results as a pandas DataFrame"""
    engine = None
    try:
        engine = get_database_connection()
        # Use SQLAlchemy text() for parameterized queries
        if params:
            df = pd.read_sql_query(text(query), engine, params=params)
        else:
            df = pd.read_sql_query(text(query), engine)
        return df
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")
    finally:
        if engine:
            engine.dispose()

@timing_decorator("Load Demand Data")
def load_demand_data():
    """Load demand data from database"""
    query = """
    SELECT
		so.ORDER_NO AS 'SO No',
		so.PART_NO AS 'Part No',
		ipt.PLANNER_BUYER AS 'Planner',
		ipt.PRIME_COMMODITY AS 'Comm Group',
		so.ROWSTATE AS 'Status',
		so.REVISED_START_DATE AS 'Start Date',
		so.REVISED_QTY_DUE AS 'Rev Qty Due',
		CASE WHEN RIGHT(so.PART_NO,1) = 'S' THEN 'Sterile' ELSE 'Non-Sterile' END AS 'Sterility'
    FROM IFS.SHOP_ORD_TAB AS so
	INNER JOIN IFS.INVENTORY_PART_TAB AS ipt
		ON so.CONTRACT = ipt.CONTRACT AND so.PART_NO = ipt.PART_NO
	WHERE so.CONTRACT = '2051'
		AND so.ROWSTATE IN ('Planned')
		AND ipt.PLANNER_BUYER IN ('3001','3801','5001','KIT SAMPLES','3802','3803','3804','3805')
	ORDER BY so.REVISED_START_DATE, so.ORDER_NO
	;
    """
    return execute_query(query)

@timing_decorator("Load Planned Demand Data")
def load_planned_demand_data():
    """Load planned demand (BOM) data from database"""
    query = """
    SELECT
		mac.ORDER_NO AS 'SO Number',
		so.PART_NO AS 'Kit Number',
		mac.PART_NO AS 'Component Part Number',
		mac.LINE_ITEM_NO AS 'Line Number',
		mac.DATE_REQUIRED AS 'Date Needed',
		mac.QTY_REQUIRED AS 'Component Qty Required'
	FROM IFS.SHOP_MATERIAL_ALLOC_TAB AS mac
	LEFT JOIN IFS.SHOP_ORD_TAB AS so
		ON mac.ORDER_NO = so.ORDER_NO AND mac.CONTRACT = so.CONTRACT
	WHERE
		so.CONTRACT = '2051'
		AND mac.ROWSTATE IN ('Planned')
	ORDER BY so.REVISED_START_DATE, mac.ORDER_NO
    """
    return execute_query(query)

@timing_decorator("Load Component Demand Data")
def load_component_demand_data():
    """Load committed component demand data from database"""
    query = """
    SELECT
		mac.ORDER_NO AS 'SO Number',
		so.PART_NO AS 'Kit Number',
		mac.PART_NO AS 'Component Part Number',
		mac.LINE_ITEM_NO AS 'Line Number',
		mac.DATE_REQUIRED AS 'Date Needed',
		mac.QTY_REQUIRED AS 'Component Qty Required'
	FROM IFS.SHOP_MATERIAL_ALLOC_TAB AS mac
	LEFT JOIN IFS.SHOP_ORD_TAB AS so
		ON mac.ORDER_NO = so.ORDER_NO AND mac.CONTRACT = so.CONTRACT
	WHERE
		so.CONTRACT = '2051'
		AND mac.ROWSTATE IN ('Released','Reserved')
	ORDER BY so.REVISED_START_DATE, mac.ORDER_NO
	"""
    return execute_query(query)

@timing_decorator("Load IPIS Data")
def load_ipis_data():
    """Load stock data from IPIS table"""
    query = """
    SELECT
		ipt.PART_NO,
		ipt.LOCATION_NO,
		ipt.LOT_BATCH_NO,
		ipt.QTY_ONHAND - ipt.QTY_RESERVED AS 'Available Qty'
	FROM IFS.INVENTORY_PART_IN_STOCK_TAB AS ipt
	WHERE 
		ipt.CONTRACT = '2051'
		AND ipt.WAREHOUSE <> 'QUALITY'
		AND ipt.AVAILABILITY_CONTROL_ID IS NULL
	UNION
	SELECT
		ipt.PART_NO,
		ipt.LOCATION_NO,
		ipt.LOT_BATCH_NO,
		ipt.QTY_ONHAND - ipt.QTY_RESERVED AS 'Available Qty'
	FROM IFS.INVENTORY_PART_IN_STOCK_TAB AS ipt
	WHERE 
		ipt.CONTRACT = '2051'
		AND ipt.WAREHOUSE <> 'QUALITY'
		AND ipt.AVAILABILITY_CONTROL_ID IN ('GOODS-INWARDS')
	"""
    return execute_query(query)

@timing_decorator("Load Hours Data")
def load_hours_data():
    """Load labor standards data from database"""
    query = """
    SELECT
		hrs.PART_NO,
		hrs.LABOR_CLASS_NO,
		CASE
			WHEN hrs.RUN_TIME_CODE IN (1,3) THEN hrs.LABOR_RUN_FACTOR
			WHEN hrs.RUN_TIME_CODE IN (2) THEN ISNULL(1/NULLIF(hrs.LABOR_RUN_FACTOR,0),0)
			ELSE 'Error' END AS 'Hours per Unit',
		'Kits' AS 'Area'
	FROM IFS.ROUTING_OPERATION_TAB AS hrs
	WHERE CONTRACT = '2051'
		AND PHASE_OUT_DATE IS NULL
		AND LABOR_CLASS_NO IN ('4936', '4948')
	UNION
	SELECT
		hrs.PART_NO,
		hrs.LABOR_CLASS_NO,
		CASE
			WHEN hrs.RUN_TIME_CODE IN (1,3) THEN hrs.LABOR_RUN_FACTOR
			WHEN hrs.RUN_TIME_CODE IN (2) THEN ISNULL(1/NULLIF(hrs.LABOR_RUN_FACTOR,0),0)
			ELSE 'Error' END AS 'Hours per Unit',
		CASE
			WHEN hrs.LABOR_CLASS_NO IN ('4931') THEN 'Manufacturing'
			WHEN hrs.LABOR_CLASS_NO IN ('4940') THEN 'Assembly'
			WHEN hrs.LABOR_CLASS_NO IN ('4941', '4947') THEN 'Packaging'
			WHEN hrs.LABOR_CLASS_NO IN ('4942') THEN 'Boxing'
			ELSE 'N/A' END AS 'Area'
	FROM IFS.ROUTING_OPERATION_TAB AS hrs
	WHERE CONTRACT = '2051'
	AND PHASE_OUT_DATE IS NULL
	AND LABOR_CLASS_NO IN ('4931', '4940', '4941', '4942', '4947');
    """
    return execute_query(query)

@timing_decorator("Load POs Data")
def load_pos_data():
    """Load purchase orders data from database"""
    query = """
    DECLARE @POWeeksOut INT;
	SET @POWeeksOut = 7;
	SELECT
		pol.ORDER_NO AS 'PO Number',
		pol.PART_NO AS 'Part Number',
		pol.BUY_QTY_DUE * pol.CONV_FACTOR AS 'Qty Due',
		pol.INVOICING_SUPPLIER AS 'Supplier',
		pol.LINE_NO AS 'PO Line',
		pol.PROMISED_DELIVERY_DATE AS 'Promised Due Date'
	FROM IFS.PURCHASE_ORDER_LINE_TAB AS pol
	INNER JOIN IFS.PURCHASE_ORDER_TAB AS po
		ON pol.ORDER_NO = po.ORDER_NO
	WHERE pol.PURCHASE_SITE = '2051'
		AND pol.ROWSTATE = 'Confirmed'
		AND pol.PROMISED_DELIVERY_DATE <= DATEADD(DAY, (@POWeeksOut+1)*7 - DATEPART(WEEKDAY, GETDATE()), GETDATE())
		AND pol.INVOICING_SUPPLIER NOT IN ('1060')
		ORDER BY pol.PROMISED_DELIVERY_DATE
    """
    return execute_query(query)

def test_database_connection():
    """Test the database connection and return status"""
    try:
        engine = get_database_connection()
        if engine:
            # Test the connection by executing a simple query
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            engine.dispose()
            return True, "Database connection successful"
    except Exception as e:
        return False, f"Database connection failed: {str(e)}"
    return False, "Database connection failed: Unknown error"

def get_table_info():
    """Get information about available tables in SQL Server"""
    try:
        engine = get_database_connection()
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
            """))
            tables = [row[0] for row in result.fetchall()]
        engine.dispose()
        return tables
    except Exception as e:
        return []

def normalize_so_number(so_val):
    """Normalize SO numbers to handle Excel float formatting issues"""
    if pd.isna(so_val):
        return ""
    so_str = str(so_val).strip()
    # Remove trailing .0 if it's a whole number
    if so_str.endswith('.0') and so_str.replace('.0', '').isdigit():
        return so_str.replace('.0', '')
    return so_str

def safe_metric(metrics, key, default=0):
    """Safely get a metric value with a default if missing"""
    return metrics.get(key, default)

def format_metric(value, format_type='number'):
    """Format metric values consistently"""
    try:
        if format_type == 'number':
            return f"{value:,}"
        elif format_type == 'hours':
            return f"{value:,.1f}"
        elif format_type == 'percentage':
            return f"{value:.1f}%"
        return str(value)
    except (TypeError, ValueError):
        return "0"  # Safe default for invalid values

def get_sorting_strategies():
    """Define all sorting strategies for min/max optimization"""
    return [
        {"name": "Start Date (Early First)", "columns": ["Start Date", "SO Number"], "ascending": [True, True]},
        {"name": "Start Date (Late First)", "columns": ["Start Date", "SO Number"], "ascending": [False, True]},
        {"name": "Demand (Small First)", "columns": ["Demand", "Start Date"], "ascending": [True, True]},
        {"name": "Demand (Large First)", "columns": ["Demand", "Start Date"], "ascending": [False, True]},
        {"name": "Hours (Quick First)", "columns": ["Hours_Calc", "Start Date"], "ascending": [True, True]},
        {"name": "Hours (Long First)", "columns": ["Hours_Calc", "Start Date"], "ascending": [False, True]},
        {"name": "Part Number (A-Z)", "columns": ["Part", "Start Date"], "ascending": [True, True]},
        {"name": "Part Number (Z-A)", "columns": ["Part", "Start Date"], "ascending": [False, True]},
        {"name": "Planner (A-Z)", "columns": ["Planner", "Start Date"], "ascending": [True, True]},
        {"name": "Planner (Z-A)", "columns": ["Planner", "Start Date"], "ascending": [False, True]}
    ]

@timing_decorator("Build Stock Dictionary")
def build_stock_dictionary(df_ipis):
    """Build stock dict using IPIS as primary source"""
    stock = {}
    
    # Use IPIS as the authoritative source
    if not df_ipis.empty:
        df_ipis["PART_NO"] = df_ipis["PART_NO"].astype(str)
        ipis_stock = df_ipis.groupby("PART_NO")["Available Qty"].sum().to_dict()
        stock.update(ipis_stock)
    else:
        print("WARNING: IPIS sheet is empty - no stock data available!")
    
    return stock 

# Processing worker thread for Qt6
class ProcessingWorker(QThread):
    progress_updated = pyqtSignal(str)
    finished = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)
    
    def __init__(self, minmax_mode, include_kits, include_instruments, include_virtuoso, include_kit_samples):
        super().__init__()
        self.minmax_mode = minmax_mode
        self.include_kits = include_kits
        self.include_instruments = include_instruments
        self.include_virtuoso = include_virtuoso
        self.include_kit_samples = include_kit_samples
        self.scenarios = []
        self.scenarios_for_comparison = []
        
    def run(self):
        try:
            # Reset performance tracker for this run
            performance_tracker.cleanup()
            
            # Test database connection first
            connection_success, connection_message = test_database_connection()
            if not connection_success:
                self.error_occurred.emit(f"Database Connection Error: {connection_message}")
                return
            
            self.progress_updated.emit("🔄 Initializing database connection...")
            
            start_time = time.time()
            
            # Determine processing mode
            if self.minmax_mode:
                # Min/Max optimization mode - test all sorting strategies
                strategies = get_sorting_strategies()
                total_scenarios = len(strategies)
                
                self.progress_updated.emit(f"🔥 MIN/MAX MODE: Testing {len(strategies)} sorting strategies on database = {total_scenarios} total scenarios")
                
                scenario_num = 0
                all_strategy_results = []  # Store ALL results for comparison
                
                for strategy_idx, strategy in enumerate(strategies):
                    scenario_num += 1
                    scenario_name = f"Database_{strategy['name'].replace(' ', '_').replace('(', '').replace(')', '')}"
                    
                    # Process with specific sorting strategy
                    scenario_start_time = time.time()
                    scenario_result = self.process_single_scenario(
                        scenario_name, scenario_num, total_scenarios, strategy
                    )
                    scenario_end_time = time.time()
                    scenario_duration = scenario_end_time - scenario_start_time
                    
                    # Store result for this strategy
                    all_strategy_results.append(scenario_result)
                    
                    # Show completion
                    metrics = scenario_result['metrics']
                    remaining_scenarios = total_scenarios - scenario_num
                    
                    if remaining_scenarios > 0:
                        total_elapsed = time.time() - start_time
                        avg_time_per_scenario = total_elapsed / scenario_num
                        estimated_remaining = remaining_scenarios * avg_time_per_scenario
                        
                        self.progress_updated.emit(f"✅ [{scenario_num}/{total_scenarios}] {strategy['name']}: {metrics['releasable_count']:,}/{metrics['total_orders']:,} orders ({scenario_duration:.1f}s) | {estimated_remaining:.0f}s remaining")
                    else:
                        self.progress_updated.emit(f"✅ [{scenario_num}/{total_scenarios}] {strategy['name']}: {metrics['releasable_count']:,}/{metrics['total_orders']:,} orders ({scenario_duration:.1f}s) | OPTIMIZATION COMPLETE!")
                    
                    time.sleep(0.2)  # Brief pause between strategies
                
                # Find the best strategies
                best_orders_strategy = max(all_strategy_results, key=lambda s: s['metrics']['releasable_count'])
                best_hours_strategy = max(all_strategy_results, key=lambda s: s['metrics']['releasable_hours'])
                best_qty_strategy = max(all_strategy_results, key=lambda s: s['metrics']['releasable_qty'])
                
                # Create NEW scenario objects with clear names for the best strategies
                # Best Orders Strategy
                best_orders_scenario = {
                    'name': f"BEST_ORDERS_Database",
                    'filepath': 'Database',
                    'sorting_strategy': f"🏆 BEST ORDERS: {best_orders_strategy['sorting_strategy']}",
                    'results_df': best_orders_strategy['results_df'],
                    'metrics': best_orders_strategy['metrics']
                }
                self.scenarios.append(best_orders_scenario)
                
                # Best Hours Strategy
                best_hours_scenario = {
                    'name': f"BEST_HOURS_Database",
                    'filepath': 'Database',
                    'sorting_strategy': f"🏆 BEST HOURS: {best_hours_strategy['sorting_strategy']}",
                    'results_df': best_hours_strategy['results_df'],
                    'metrics': best_hours_strategy['metrics']
                }
                self.scenarios.append(best_hours_scenario)
                
                # Best Quantity Strategy
                best_qty_scenario = {
                    'name': f"BEST_QTY_Database",
                    'filepath': 'Database',
                    'sorting_strategy': f"🏆 BEST QTY: {best_qty_strategy['sorting_strategy']}",
                    'results_df': best_qty_strategy['results_df'],
                    'metrics': best_qty_strategy['metrics']
                }
                self.scenarios.append(best_qty_scenario)
                
                self.progress_updated.emit(f"🏆 Database optimized: Orders={best_orders_strategy['sorting_strategy']} ({best_orders_strategy['metrics']['releasable_count']:,}), Hours={best_hours_strategy['sorting_strategy']} ({best_hours_strategy['metrics']['releasable_hours']:,.0f}), Qty={best_qty_strategy['sorting_strategy']} ({best_qty_strategy['metrics']['releasable_qty']:,})")
                time.sleep(0.5)
                
                # Use all_strategy_results for comparison tables
                self.scenarios_for_comparison = all_strategy_results
            else:
                # Standard mode - process database once
                total_scenarios = 1
                
                scenario_name = "Database_Scenario"
                scenario_num = 1
                
                self.progress_updated.emit(f"📊 [Scenario {scenario_num}/{total_scenarios}] Starting: Database Analysis")
                
                # Process with live progress updates
                scenario_start_time = time.time()
                scenario_result = self.process_single_scenario(
                    scenario_name, scenario_num, total_scenarios
                )
                scenario_end_time = time.time()
                scenario_duration = scenario_end_time - scenario_start_time
                
                self.scenarios.append(scenario_result)
                self.scenarios_for_comparison.append(scenario_result)  # Same as scenarios in standard mode
                
                # Show completion with actual metrics and time
                metrics = scenario_result['metrics']
                self.progress_updated.emit(f"✅ [Scenario {scenario_num}/{total_scenarios}] Complete: {metrics['releasable_count']:,}/{metrics['total_orders']:,} releasable ({scenario_duration:.1f}s) | COMPLETE!")
                
                time.sleep(0.3)
            
            # Calculate total processing time
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Return results
            results = {
                'scenarios': self.scenarios,
                'scenarios_for_comparison': self.scenarios_for_comparison,
                'processing_time': processing_time,
                'minmax_mode': self.minmax_mode
            }
            
            self.finished.emit(results)
            
        except Exception as e:
            self.error_occurred.emit(f"Processing failed: {str(e)}")
    
    def process_single_scenario(self, scenario_name, scenario_num=1, total_scenarios=1, sorting_strategy=None):
        """Process a single scenario from database and return results with live progress updates"""
        
        # Start overall processing timing
        performance_tracker.start_phase("Total Processing")
        
        strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
        self.progress_updated.emit(f"📂 [Scenario {scenario_num}/{total_scenarios}] Loading data from database ({strategy_name})...")
        
        # Load the data from database with proper timing separation
        try:
            # Test database connection first (this will be included in the data loading timing)
            performance_tracker.start_phase("Database Connection")
            test_connection = get_database_connection()
            performance_tracker.end_phase()
            
            # Load all data tables
            df_main = load_demand_data()
            df_struct = load_planned_demand_data()
            df_component_demand = load_component_demand_data()
            df_ipis = load_ipis_data()
            df_hours = load_hours_data()
            df_pos = load_pos_data()
        except Exception as e:
            performance_tracker.end_phase()  # End total processing
            raise Exception(f"Failed to load data from database: {str(e)}")
        
        # Validate required columns in Demand table
        required_columns = ['SO No', 'Part No', 'Planner', 'Start Date', 'Rev Qty Due']
        missing_columns = [col for col in required_columns if col not in df_main.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in Demand table: {', '.join(missing_columns)}")
        
        # Rename columns to match the rest of the code's expectations
        df_main = df_main.rename(columns={
            'SO No': 'SO Number',
            'Part No': 'Part',
            'Rev Qty Due': 'Demand'
        })
        
        strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
        self.progress_updated.emit(f"🔁 [Scenario {scenario_num}/{total_scenarios}] Processing commitments ({strategy_name})...")
        
        # Build stock dictionary
        stock = build_stock_dictionary(df_ipis)
        
        # Build committed_components
        committed_components = {}
        committed_parts_count = 0
        total_committed_qty = 0

        if not df_component_demand.empty:
            df_component_demand["Component Part Number"] = df_component_demand["Component Part Number"].astype(str)
            committed_summary = df_component_demand.groupby("Component Part Number")["Component Qty Required"].sum()
            committed_components = committed_summary.to_dict()
            committed_parts_count = len(committed_components)
            total_committed_qty = sum(committed_components.values())

        # Initialize used_components with the committed quantities
        used_components = committed_components.copy()

        # Build labor standards dictionary (unchanged)
        df_hours["PART_NO"] = df_hours["PART_NO"].astype(str)
        labor_standards = df_hours.groupby("PART_NO")["Hours per Unit"].sum().to_dict()
        
        strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
        self.progress_updated.emit(f"🔁 [Scenario {scenario_num}/{total_scenarios}] Building planned demand structures ({strategy_name})...")
        
        # Build planned demand structure
        planned_demand = df_struct[df_struct["Component Part Number"].notna()].copy()
        planned_demand["SO Number"] = planned_demand["SO Number"].astype(str).str.strip()
        planned_demand["Component Part Number"] = planned_demand["Component Part Number"].astype(str)
        
        # Apply normalization to both planned demand and main data
        planned_demand["SO Number"] = planned_demand["SO Number"].apply(normalize_so_number)
        
        # Pre-process main data (unchanged)
        df_main['Start Date'] = pd.to_datetime(df_main['Start Date'], errors='coerce')
        df_main["Part"] = df_main["Part"].astype(str)
        df_main["Planner"] = df_main["Planner"].fillna("UNKNOWN").astype(str)
        df_main["Demand"] = pd.to_numeric(df_main["Demand"], errors='coerce').fillna(0)
        
        # Filter data based on selected categories
        filtered_df_main = df_main.copy()
        
        # Define planner codes for each category
        kits_planners = ['3001', '3801', '5001']  # BVI Kits (3001, 3801) + Malosa Kits (5001)
        instruments_planners = ['3802', '3803', '3804', '3805']  # Manufacturing, Assembly, Packaging, Malosa Instruments
        virtuoso_planners = ['3806']  # Virtuoso
        kit_samples_planners = ['KIT SAMPLES']
        
        # Build filter mask based on selected categories
        filter_mask = pd.Series([False] * len(df_main), index=df_main.index)
        
        if self.include_kits:
            filter_mask |= df_main['Planner'].isin(kits_planners)
        
        if self.include_instruments:
            filter_mask |= df_main['Planner'].isin(instruments_planners)
        
        if self.include_virtuoso:
            filter_mask |= df_main['Planner'].isin(virtuoso_planners)
        
        if self.include_kit_samples:
            filter_mask |= df_main['Planner'].isin(kit_samples_planners)
        
        # Apply filter
        filtered_df_main = df_main[filter_mask].copy()
        
        total_original = len(df_main)
        total_filtered = len(filtered_df_main)
        excluded = total_original - total_filtered
        self.progress_updated.emit(f"🔁 [Scenario {scenario_num}/{total_scenarios}] Filtered data: {total_filtered:,}/{total_original:,} orders selected ({excluded:,} excluded) ({strategy_name})...")
        
        # Calculate hours for sorting (unchanged)
        filtered_df_main["Hours_Calc"] = filtered_df_main.apply(lambda row: 
            labor_standards.get(str(row["Part"]), 0) * row["Demand"], axis=1)
        
        # Apply sorting strategy (unchanged)
        if sorting_strategy:
            # Handle missing values appropriately for each column type
            for col in sorting_strategy["columns"]:
                if col == "Start Date":
                    # Put NaT (missing dates) at the end
                    filtered_df_main = filtered_df_main.sort_values(sorting_strategy["columns"], 
                                                ascending=sorting_strategy["ascending"], 
                                                na_position='last')
                else:
                    filtered_df_main = filtered_df_main.sort_values(sorting_strategy["columns"], 
                                                ascending=sorting_strategy["ascending"])
        else:
            # Default sorting (original behavior)
            filtered_df_main = filtered_df_main.sort_values(['Start Date', 'SO Number'], na_position='last')
        
        filtered_df_main = filtered_df_main.reset_index(drop=True)
        
        results = []
        # Start order processing timing
        performance_tracker.start_phase("Order Processing")
        
        processed = 0
        total = len(filtered_df_main)
        
        # Baseline estimate: ~0.15 seconds per order (conservative estimate)
        baseline_time_per_order = 0.15
        processing_start_time = time.time()

        # Process each order sequentially with FREQUENT UI updates + TIME ESTIMATES
        for _, row in filtered_df_main.iterrows():
            processed += 1
            
            # UPDATE UI EVERY 100 ORDERS
            if processed % 100 == 0 or processed == total or processed == 1:
                progress_pct = processed / total * 100
                
                # Calculate dynamic time estimates
                if processed >= 10:  # After 10 orders, use actual performance
                    elapsed = time.time() - processing_start_time
                    actual_time_per_order = elapsed / processed
                    remaining_orders = total - processed
                    est_remaining = remaining_orders * actual_time_per_order
                else:  # For first few orders, use baseline estimate
                    elapsed = time.time() - processing_start_time
                    remaining_orders = total - processed
                    est_remaining = remaining_orders * baseline_time_per_order
                
                strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
                if processed == total:
                    self.progress_updated.emit(f"✅ [Scenario {scenario_num}/{total_scenarios}] Database ({strategy_name}) - Completed {total:,} orders in {elapsed:.1f}s")
                else:
                    # Show current scenario progress + context about remaining scenarios
                    remaining_scenarios = total_scenarios - scenario_num
                    if remaining_scenarios > 0:
                        self.progress_updated.emit(f"🔁 [Scenario {scenario_num}/{total_scenarios}] {strategy_name} - {processed:,}/{total:,} ({progress_pct:.1f}%) | {est_remaining:.0f}s + {remaining_scenarios} more")
                    else:
                        self.progress_updated.emit(f"🔁 [Scenario {scenario_num}/{total_scenarios}] {strategy_name} - {processed:,}/{total:,} ({progress_pct:.1f}%) | {est_remaining:.0f}s remaining")
            
            so = str(row["SO Number"]).strip() if pd.notna(row["SO Number"]) else f"ORDER_{processed}"
            part = str(row["Part"]) if pd.notna(row["Part"]) else None
            demand_qty = row["Demand"] if pd.notna(row["Demand"]) and row["Demand"] > 0 else 0
            planner = str(row["Planner"]) if pd.notna(row["Planner"]) else "UNKNOWN"
            start_date = row["Start Date"]
            
            # NORMALIZE SO NUMBER for consistent matching
            so = normalize_so_number(so)
            
            # Skip orders with missing critical data
            if part is None or part == "nan" or demand_qty <= 0:
                results.append({
                    "SO Number": so,
                    "Part": part or "MISSING",
                    "Planner": planner,
                    "Start Date": start_date.strftime('%Y-%m-%d') if pd.notna(start_date) else "No Date",
                    "PB": "-",
                    "Demand": demand_qty,
                    "Hours": 0,
                    "Status": "⚠️ Skipped",
                    "Shortages": "-",
                    "Components": "Missing part number or zero demand"
                })
                continue
            
            # Check if this is a piggyback order
            try:
                pb_check = f"NS{part}99"
                is_pb = "PB" if pb_check in planned_demand["Component Part Number"].values else "-"
            except:
                is_pb = "-"
            
            # Get planned demand for this SO
            try:
                bom = planned_demand[planned_demand["SO Number"] == so]
            except:
                bom = pd.DataFrame()
            
            # Check material availability
            releasable = True
            shortage_details = []
            components_needed = {}
            
            # Calculate labor hours for this order
            base_hours = labor_standards.get(part, 0)
            labor_hours = base_hours * demand_qty
            
            if len(bom) > 0:
                # This SO has planned component demand - use ALL-OR-NOTHING allocation
                all_components_available = True
                component_requirements = []
                
                for _, comp in bom.iterrows():
                    try:
                        comp_part = str(comp["Component Part Number"])
                        required_qty = int(comp["Component Qty Required"]) if pd.notna(comp["Component Qty Required"]) else 0
                        total_used = used_components.get(comp_part, 0)
                        true_available = stock.get(comp_part, 0) - total_used
                        available = max(0, true_available)

                        # Always use the 'what if' calculation for both debug and output
                        available_after_usage = true_available - required_qty
                        will_be_sufficient = true_available >= required_qty

                        component_requirements.append({
                            'part': comp_part,
                            'required': required_qty,
                            'available': available
                        })
                        components_needed[comp_part] = required_qty

                        # Check availability but DON'T allocate yet
                        if not will_be_sufficient:
                            all_components_available = False
                            shortage = abs(available_after_usage)  # Changed to use available_after_usage directly
                            # Search POs for potential resolution
                            future_pos = df_pos[
                                (df_pos["Part Number"].astype(str) == comp_part) &
                                (pd.to_datetime(df_pos["Promised Due Date"], errors="coerce") >= datetime.now())
                            ]
                            po_match = None
                            for _, po_row in future_pos.iterrows():
                                po_qty = po_row["Qty Due"]
                                if po_qty >= shortage:
                                    po_match = po_row
                                    break
                            if po_match is not None:
                                po_id = po_match["PO Number"]
                                po_date = pd.to_datetime(po_match["Promised Due Date"]).strftime('%Y-%m-%d')
                                shortage_details.append(f"{comp_part} short {shortage} – PO {po_id} due {po_date}")
                            else:
                                shortage_details.append(f"{comp_part} (need {required_qty}, have {true_available}, short {shortage})")
                    except Exception as e:
                        all_components_available = False
                        shortage_details.append(f"Component processing error: {str(e)}")
                        continue

                # Only after all checks, allocate components if all are available
                if all_components_available:
                    releasable = True
                    for req in component_requirements:
                        comp_part = req['part']
                        required_qty = req['required']
                        used_components[comp_part] = used_components.get(comp_part, 0) + required_qty
                else:
                    releasable = False

            elif len(bom) == 0:
                # This SO has no planned component demand - treat as raw material/purchased part
                try:
                    total_used = used_components.get(part, 0)
                    true_available = stock.get(part, 0) - total_used  # Changed to use true_available
                    available_after_usage = true_available - demand_qty  # Added to match debug logic
                    
                    if true_available >= demand_qty:  # Changed to use true_available
                        used_components[part] = used_components.get(part, 0) + demand_qty
                        releasable = True
                    else:
                        releasable = False
                        shortage = abs(available_after_usage)  # Changed to use available_after_usage
                        shortage_details.append(f"{part} (need {demand_qty}, have {true_available}, short {shortage})")
                except:
                    releasable = False
                    shortage_details.append(f"{part} (stock lookup failed)")

            # Build result record
            shortage_parts_only = []
            components_info = "; ".join(shortage_details) if shortage_details else str(components_needed) if components_needed else "-"

            # Extract just the part numbers from shortage details
            for detail in shortage_details:
                if " short " in detail and "–" in detail:
                    part_short = detail.split(" short ")[0].strip()
                    shortage_parts_only.append(part_short)
                elif "(" in detail and " (need " in detail:
                    part_short = detail.split(" (need ")[0].strip()
                    shortage_parts_only.append(part_short)
                elif "(" in detail:
                    part_short = detail.split("(")[0].strip()
                    shortage_parts_only.append(part_short)
                else:
                    part_short = detail.split()[0] if detail.split() else detail
                    shortage_parts_only.append(part_short)

            clean_shortages = "; ".join(shortage_parts_only) if shortage_parts_only else "-"

            results.append({
                "SO Number": so,
                "Part": part,
                "Planner": planner,
                "Start Date": start_date.strftime('%Y-%m-%d') if pd.notna(start_date) else "No Date",
                "PB": is_pb,
                "Demand": demand_qty,
                "Hours": round(labor_hours, 4),
                "Status": "✅ Release" if releasable else "❌ Hold",
                "Shortages": clean_shortages,
                "Components": components_info
            })

        # Calculate summary metrics
        df_results = pd.DataFrame(results)
        total_orders = len(df_results)
        releasable_count = len(df_results[df_results['Status'] == '✅ Release'])
        held_count = total_orders - releasable_count
        pb_count = len(df_results[df_results['PB'] == 'PB'])
        skipped_count = len(df_results[df_results['Status'] == '⚠️ Skipped'])
        
        total_hours = df_results['Hours'].sum()
        releasable_hours = df_results[df_results['Status'] == '✅ Release']['Hours'].sum()
        held_hours = df_results[df_results['Status'] == '❌ Hold']['Hours'].sum()
        
        # Calculate quantity metrics
        total_qty = df_results['Demand'].sum()
        releasable_qty = df_results[df_results['Status'] == '✅ Release']['Demand'].sum()
        held_qty = df_results[df_results['Status'] == '❌ Hold']['Demand'].sum()
        
        # Calculate Kit and Instrument metrics with subcategories
        releasable_results = df_results[df_results['Status'] == '✅ Release']
        held_results = df_results[df_results['Status'] == '❌ Hold']
        
        # BVI Kits (Planner codes 3001, 3801)
        bvi_kit_planners = ['3001', '3801']
        total_bvi_kits = df_results[df_results['Planner'].isin(bvi_kit_planners)]
        releasable_bvi_kits = releasable_results[releasable_results['Planner'].isin(bvi_kit_planners)]
        total_bvi_kits_count = len(total_bvi_kits)
        total_bvi_kits_hours = total_bvi_kits['Hours'].sum()
        total_bvi_kits_qty = total_bvi_kits['Demand'].sum()
        releasable_bvi_kits_count = len(releasable_bvi_kits)
        releasable_bvi_kits_hours = releasable_bvi_kits['Hours'].sum()
        releasable_bvi_kits_qty = releasable_bvi_kits['Demand'].sum()
        
        # Malosa Kits (Planner code 5001)
        malosa_kit_planners = ['5001']
        total_malosa_kits = df_results[df_results['Planner'].isin(malosa_kit_planners)]
        releasable_malosa_kits = releasable_results[releasable_results['Planner'].isin(malosa_kit_planners)]
        total_malosa_kits_count = len(total_malosa_kits)
        total_malosa_kits_hours = total_malosa_kits['Hours'].sum()
        total_malosa_kits_qty = total_malosa_kits['Demand'].sum()
        releasable_malosa_kits_count = len(releasable_malosa_kits)
        releasable_malosa_kits_hours = releasable_malosa_kits['Hours'].sum()
        releasable_malosa_kits_qty = releasable_malosa_kits['Demand'].sum()
        
        # Total Kits
        total_kits_count = total_bvi_kits_count + total_malosa_kits_count
        total_kits_hours = total_bvi_kits_hours + total_malosa_kits_hours
        total_kits_qty = total_bvi_kits_qty + total_malosa_kits_qty
        releasable_kits_count = releasable_bvi_kits_count + releasable_malosa_kits_count
        releasable_kits_hours = releasable_bvi_kits_hours + releasable_malosa_kits_hours
        releasable_kits_qty = releasable_bvi_kits_qty + releasable_malosa_kits_qty
        
        # Manufacturing (Planner code 3802)
        manufacturing_planners = ['3802']
        total_manufacturing = df_results[df_results['Planner'].isin(manufacturing_planners)]
        releasable_manufacturing = releasable_results[releasable_results['Planner'].isin(manufacturing_planners)]
        total_manufacturing_count = len(total_manufacturing)
        total_manufacturing_hours = total_manufacturing['Hours'].sum()
        total_manufacturing_qty = total_manufacturing['Demand'].sum()
        releasable_manufacturing_count = len(releasable_manufacturing)
        releasable_manufacturing_hours = releasable_manufacturing['Hours'].sum()
        releasable_manufacturing_qty = releasable_manufacturing['Demand'].sum()
        
        # Assembly (Planner code 3803)
        assembly_planners = ['3803']
        total_assembly = df_results[df_results['Planner'].isin(assembly_planners)]
        releasable_assembly = releasable_results[releasable_results['Planner'].isin(assembly_planners)]
        total_assembly_count = len(total_assembly)
        total_assembly_hours = total_assembly['Hours'].sum()
        total_assembly_qty = total_assembly['Demand'].sum()
        releasable_assembly_count = len(releasable_assembly)
        releasable_assembly_hours = releasable_assembly['Hours'].sum()
        releasable_assembly_qty = releasable_assembly['Demand'].sum()
        
        # Packaging (Planner code 3804)
        packaging_planners = ['3804']
        total_packaging = df_results[df_results['Planner'].isin(packaging_planners)]
        releasable_packaging = releasable_results[releasable_results['Planner'].isin(packaging_planners)]
        total_packaging_count = len(total_packaging)
        total_packaging_hours = total_packaging['Hours'].sum()
        total_packaging_qty = total_packaging['Demand'].sum()
        releasable_packaging_count = len(releasable_packaging)
        releasable_packaging_hours = releasable_packaging['Hours'].sum()
        releasable_packaging_qty = releasable_packaging['Demand'].sum()
        
        # Malosa Instruments (Planner code 3805)
        malosa_instrument_planners = ['3805']
        total_malosa_instruments = df_results[df_results['Planner'].isin(malosa_instrument_planners)]
        releasable_malosa_instruments = releasable_results[releasable_results['Planner'].isin(malosa_instrument_planners)]
        total_malosa_instruments_count = len(total_malosa_instruments)
        total_malosa_instruments_hours = total_malosa_instruments['Hours'].sum()
        total_malosa_instruments_qty = total_malosa_instruments['Demand'].sum()
        releasable_malosa_instruments_count = len(releasable_malosa_instruments)
        releasable_malosa_instruments_hours = releasable_malosa_instruments['Hours'].sum()
        releasable_malosa_instruments_qty = releasable_malosa_instruments['Demand'].sum()
        
        # Virtuoso (Planner code 3806)
        virtuoso_planners = ['3806']
        total_virtuoso = df_results[df_results['Planner'].isin(virtuoso_planners)]
        releasable_virtuoso = releasable_results[releasable_results['Planner'].isin(virtuoso_planners)]
        total_virtuoso_count = len(total_virtuoso)
        total_virtuoso_hours = total_virtuoso['Hours'].sum()
        total_virtuoso_qty = total_virtuoso['Demand'].sum()
        releasable_virtuoso_count = len(releasable_virtuoso)
        releasable_virtuoso_hours = releasable_virtuoso['Hours'].sum()
        releasable_virtuoso_qty = releasable_virtuoso['Demand'].sum()
        
        # Kit Samples (Planner code KIT SAMPLES)
        kit_samples_planners = ['KIT SAMPLES']
        total_kit_samples = df_results[df_results['Planner'].isin(kit_samples_planners)]
        releasable_kit_samples = releasable_results[releasable_results['Planner'].isin(kit_samples_planners)]
        total_kit_samples_count = len(total_kit_samples)
        total_kit_samples_hours = total_kit_samples['Hours'].sum()
        total_kit_samples_qty = total_kit_samples['Demand'].sum()
        releasable_kit_samples_count = len(releasable_kit_samples)
        releasable_kit_samples_hours = releasable_kit_samples['Hours'].sum()
        releasable_kit_samples_qty = releasable_kit_samples['Demand'].sum()
        
        # Total Instruments
        total_instruments_count = (total_manufacturing_count + total_assembly_count + 
                                 total_packaging_count + total_malosa_instruments_count)
        total_instruments_hours = (total_manufacturing_hours + total_assembly_hours + 
                                 total_packaging_hours + total_malosa_instruments_hours)
        total_instruments_qty = (total_manufacturing_qty + total_assembly_qty + 
                               total_packaging_qty + total_malosa_instruments_qty)
        releasable_instruments_count = (releasable_manufacturing_count + releasable_assembly_count + 
                                      releasable_packaging_count + releasable_malosa_instruments_count)
        releasable_instruments_hours = (releasable_manufacturing_hours + releasable_assembly_hours + 
                                      releasable_packaging_hours + releasable_malosa_instruments_hours)
        releasable_instruments_qty = (releasable_manufacturing_qty + releasable_assembly_qty + 
                                    releasable_packaging_qty + releasable_malosa_instruments_qty)
        
        # End order processing and total processing phases
        performance_tracker.end_phase()  # End Order Processing
        performance_tracker.end_phase()  # End Total Processing
        
        return {
            'name': scenario_name,
            'filepath': 'Database',
            'sorting_strategy': sorting_strategy["name"] if sorting_strategy else "Default (Start Date)",
            'results_df': df_results,
            'metrics': {
                'total_orders': total_orders,
                'releasable_count': releasable_count,
                'held_count': held_count,
                'pb_count': pb_count,
                'skipped_count': skipped_count,
                '---1': '---',
                'total_hours': total_hours,
                'releasable_hours': releasable_hours,
                'held_hours': held_hours,
                'total_qty': total_qty,
                'releasable_qty': releasable_qty,
                'held_qty': held_qty,
                '---2': '---',
                'total_kits_count': total_kits_count,
                'total_kits_hours': total_kits_hours,
                'total_kits_qty': total_kits_qty,
                'releasable_kits_count': releasable_kits_count,
                'releasable_kits_hours': releasable_kits_hours,
                'releasable_kits_qty': releasable_kits_qty,
                'total_bvi_kits_count': total_bvi_kits_count,
                'total_bvi_kits_hours': total_bvi_kits_hours,
                'total_bvi_kits_qty': total_bvi_kits_qty,
                'releasable_bvi_kits_count': releasable_bvi_kits_count,
                'releasable_bvi_kits_hours': releasable_bvi_kits_hours,
                'releasable_bvi_kits_qty': releasable_bvi_kits_qty,
                'total_malosa_kits_count': total_malosa_kits_count,
                'total_malosa_kits_hours': total_malosa_kits_hours,
                'total_malosa_kits_qty': total_malosa_kits_qty,
                'releasable_malosa_kits_count': releasable_malosa_kits_count,
                'releasable_malosa_kits_hours': releasable_malosa_kits_hours,
                'releasable_malosa_kits_qty': releasable_malosa_kits_qty,
                '---3': '---',
                'total_instruments_count': total_instruments_count,
                'total_instruments_hours': total_instruments_hours,
                'total_instruments_qty': total_instruments_qty,
                'releasable_instruments_count': releasable_instruments_count,
                'releasable_instruments_hours': releasable_instruments_hours,
                'releasable_instruments_qty': releasable_instruments_qty,
                'total_manufacturing_count': total_manufacturing_count,
                'total_manufacturing_hours': total_manufacturing_hours,
                'total_manufacturing_qty': total_manufacturing_qty,
                'releasable_manufacturing_count': releasable_manufacturing_count,
                'releasable_manufacturing_hours': releasable_manufacturing_hours,
                'releasable_manufacturing_qty': releasable_manufacturing_qty,
                'total_assembly_count': total_assembly_count,
                'total_assembly_hours': total_assembly_hours,
                'total_assembly_qty': total_assembly_qty,
                'releasable_assembly_count': releasable_assembly_count,
                'releasable_assembly_hours': releasable_assembly_hours,
                'releasable_assembly_qty': releasable_assembly_qty,
                'total_packaging_count': total_packaging_count,
                'total_packaging_hours': total_packaging_hours,
                'total_packaging_qty': total_packaging_qty,
                'releasable_packaging_count': releasable_packaging_count,
                'releasable_packaging_hours': releasable_packaging_hours,
                'releasable_packaging_qty': releasable_packaging_qty,
                'total_malosa_instruments_count': total_malosa_instruments_count,
                'total_malosa_instruments_hours': total_malosa_instruments_hours,
                'total_malosa_instruments_qty': total_malosa_instruments_qty,
                'releasable_malosa_instruments_count': releasable_malosa_instruments_count,
                'releasable_malosa_instruments_hours': releasable_malosa_instruments_hours,
                'releasable_malosa_instruments_qty': releasable_malosa_instruments_qty,
                '---4': '---',
                'total_virtuoso_count': total_virtuoso_count,
                'total_virtuoso_hours': total_virtuoso_hours,
                'total_virtuoso_qty': total_virtuoso_qty,
                'releasable_virtuoso_count': releasable_virtuoso_count,
                'releasable_virtuoso_hours': releasable_virtuoso_hours,
                'releasable_virtuoso_qty': releasable_virtuoso_qty,
                '---5': '---',
                'committed_parts_count': committed_parts_count,
                'total_committed_qty': total_committed_qty,
                'total_kit_samples_count': total_kit_samples_count,
                'total_kit_samples_hours': total_kit_samples_hours,
                'total_kit_samples_qty': total_kit_samples_qty,
                'releasable_kit_samples_count': releasable_kit_samples_count,
                'releasable_kit_samples_hours': releasable_kit_samples_hours,
                'releasable_kit_samples_qty': releasable_kit_samples_qty
            }
        } 

# Qt6 Main Window
class PlanSnapMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.quick_analysis_excel_buffer = None
        self.processing_worker = None
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle(f"PlanSnap {VERSION} - Material Release Planning Tool")
        self.setGeometry(100, 100, 800, 700)
        
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        
        # Title
        title_label = QLabel(f"PlanSnap {VERSION}")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)
        
        # Version date
        version_label = QLabel(f"Updated: {VERSION_DATE}")
        version_font = QFont()
        version_font.setPointSize(8)
        version_label.setFont(version_font)
        version_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(version_label)
        
        # Add some spacing
        main_layout.addSpacing(20)
        
        # Min/Max Mode checkbox
        self.minmax_checkbox = QCheckBox("🔥 Enable Triple Optimization Mode")
        self.minmax_checkbox.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        main_layout.addWidget(self.minmax_checkbox)
        
        # Min/Max tooltip
        minmax_tooltip = QLabel("Tests all sorting strategies to find best results for Orders, Hours, and Quantity")
        minmax_tooltip.setFont(QFont("Arial", 8))
        minmax_tooltip.setStyleSheet("color: gray; font-style: italic;")
        main_layout.addWidget(minmax_tooltip)
        
        main_layout.addSpacing(10)
        
        # No Export checkbox
        self.no_export_checkbox = QCheckBox("⚡ Quick Analysis Mode")
        self.no_export_checkbox.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        main_layout.addWidget(self.no_export_checkbox)
        
        # No Export tooltip
        no_export_tooltip = QLabel("Show results instantly without creating Excel files (useful for rapid testing)")
        no_export_tooltip.setFont(QFont("Arial", 8))
        no_export_tooltip.setStyleSheet("color: gray; font-style: italic;")
        main_layout.addWidget(no_export_tooltip)
        
        main_layout.addSpacing(10)
        
        # Material Category Selection
        material_group = QGroupBox("🔧 Material Categories to Process")
        material_layout = QVBoxLayout(material_group)
        
        # Create variables for material category checkboxes
        self.include_kits_checkbox = QCheckBox("🔧 Kits (Planner codes: 3001, 3801, 5001)")
        self.include_kits_checkbox.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        self.include_kits_checkbox.setChecked(True)
        material_layout.addWidget(self.include_kits_checkbox)
        
        self.include_instruments_checkbox = QCheckBox("🔬 Instruments (Planner codes: 3802, 3803, 3804, 3805)")
        self.include_instruments_checkbox.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        self.include_instruments_checkbox.setChecked(True)
        material_layout.addWidget(self.include_instruments_checkbox)
        
        self.include_virtuoso_checkbox = QCheckBox("🎵 Virtuoso (Planner code: 3806)")
        self.include_virtuoso_checkbox.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        self.include_virtuoso_checkbox.setChecked(True)
        material_layout.addWidget(self.include_virtuoso_checkbox)
        
        self.include_kit_samples_checkbox = QCheckBox("🧪 Kit Samples (Planner code: KIT SAMPLES)")
        self.include_kit_samples_checkbox.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        self.include_kit_samples_checkbox.setChecked(False)
        material_layout.addWidget(self.include_kit_samples_checkbox)
        
        # Material categories tooltip
        material_tooltip = QLabel("Untick categories to exclude them from material availability checks")
        material_tooltip.setFont(QFont("Arial", 8))
        material_tooltip.setStyleSheet("color: gray; font-style: italic;")
        material_layout.addWidget(material_tooltip)
        
        main_layout.addWidget(material_group)
        
        main_layout.addSpacing(10)
        
        # Process button
        self.process_btn = QPushButton("🗄️ CONNECT TO DATABASE & PROCESS")
        self.process_btn.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.process_btn.clicked.connect(self.start_processing)
        main_layout.addWidget(self.process_btn)
        
        main_layout.addSpacing(10)
        
        # Status
        self.status_label = QLabel("🔄 Ready - Connect to database to begin processing")
        self.status_label.setFont(QFont("Arial", 10))
        main_layout.addWidget(self.status_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # Results area
        results_group = QGroupBox("📊 Results")
        results_layout = QVBoxLayout(results_group)
        
        self.results_text = QTextEdit()
        self.results_text.setFont(QFont("Consolas", 9))
        self.results_text.setReadOnly(True)
        results_layout.addWidget(self.results_text)
        
        # Buttons layout
        buttons_layout = QHBoxLayout()
        
        self.copy_btn = QPushButton("📋 Copy Summary")
        self.copy_btn.clicked.connect(self.copy_summary_to_clipboard)
        buttons_layout.addWidget(self.copy_btn)
        
        self.download_btn = QPushButton("⬇️ Download File")
        self.download_btn.clicked.connect(self.download_quick_analysis_file)
        self.download_btn.setVisible(False)
        buttons_layout.addWidget(self.download_btn)
        
        results_layout.addLayout(buttons_layout)
        main_layout.addWidget(results_group)
        
        # Show initial message
        self.results_text.setPlainText("Connect to your database to begin material release planning...")
        
    def start_processing(self):
        """Start the processing in a separate thread"""
        if self.processing_worker and self.processing_worker.isRunning():
            return  # Already processing
        
        # Disable the process button
        self.process_btn.setEnabled(False)
        self.process_btn.setText("🔄 Processing...")
        
        # Show progress bar
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        
        # Create and start the worker thread
        self.processing_worker = ProcessingWorker(
            minmax_mode=self.minmax_checkbox.isChecked(),
            include_kits=self.include_kits_checkbox.isChecked(),
            include_instruments=self.include_instruments_checkbox.isChecked(),
            include_virtuoso=self.include_virtuoso_checkbox.isChecked(),
            include_kit_samples=self.include_kit_samples_checkbox.isChecked()
        )
        
        # Connect signals
        self.processing_worker.progress_updated.connect(self.update_status)
        self.processing_worker.finished.connect(self.processing_finished)
        self.processing_worker.error_occurred.connect(self.processing_error)
        
        # Start the worker
        self.processing_worker.start()
    
    def update_status(self, message):
        """Update the status label with progress messages"""
        self.status_label.setText(message)
        QApplication.processEvents()  # Process events to update UI
    
    def processing_finished(self, results):
        """Handle processing completion"""
        # Hide progress bar
        self.progress_bar.setVisible(False)
        
        # Re-enable the process button
        self.process_btn.setEnabled(True)
        self.process_btn.setText("🗄️ CONNECT TO DATABASE & PROCESS")
        
        # Generate and display results
        self.display_results(results)
        
        # Show download button if in quick analysis mode
        if self.no_export_checkbox.isChecked():
            self.download_btn.setVisible(True)
        else:
            self.download_btn.setVisible(False)
    
    def processing_error(self, error_message):
        """Handle processing errors"""
        # Hide progress bar
        self.progress_bar.setVisible(False)
        
        # Re-enable the process button
        self.process_btn.setEnabled(True)
        self.process_btn.setText("🗄️ CONNECT TO DATABASE & PROCESS")
        
        # Show error message
        QMessageBox.critical(self, "Processing Error", error_message)
        self.status_label.setText("❌ Processing failed")
    
    def display_results(self, results):
        """Display the processing results"""
        scenarios = results['scenarios']
        scenarios_for_comparison = results['scenarios_for_comparison']
        processing_time = results['processing_time']
        minmax_mode = results['minmax_mode']
        
        # Generate summary text
        if minmax_mode:
            # Min/Max optimization summary
            files_processed = list(set([s['filepath'] for s in scenarios_for_comparison]))
            
            summary_text = f"""🔥 MIN/MAX OPTIMIZATION COMPLETE!

📊 OPTIMIZATION ANALYSIS:
   Files Analyzed: {len(files_processed)}
   Sorting Strategies Tested: {len(get_sorting_strategies())}
   Total Strategy Tests: {len(scenarios_for_comparison)}
   Best Strategies Saved: {len(scenarios)} individual sheets (3 per file: Orders, Hours, Qty)

🔧 MATERIAL CATEGORIES PROCESSED:
   Kits (3001, 3801, 5001): {'✓ Included' if self.include_kits_checkbox.isChecked() else '✗ Excluded'}
   Instruments (3802, 3803, 3804, 3805): {'✓ Included' if self.include_instruments_checkbox.isChecked() else '✗ Excluded'}
   Virtuoso (3806): {'✓ Included' if self.include_virtuoso_checkbox.isChecked() else '✗ Excluded'}
   Kit Samples (KIT SAMPLES): {'✓ Included' if self.include_kit_samples_checkbox.isChecked() else '✗ Excluded'}

"""
            
            for filepath in files_processed:
                file_scenarios = [s for s in scenarios_for_comparison if s['filepath'] == filepath]
                best_orders = max(file_scenarios, key=lambda s: s['metrics']['releasable_count'])
                best_hours = max(file_scenarios, key=lambda s: s['metrics']['releasable_hours'])
                best_qty = max(file_scenarios, key=lambda s: s['metrics']['releasable_qty'])
                worst_orders = min(file_scenarios, key=lambda s: s['metrics']['releasable_count'])
                
                improvement_orders = best_orders['metrics']['releasable_count'] - worst_orders['metrics']['releasable_count']
                improvement_pct = improvement_orders / worst_orders['metrics']['total_orders'] * 100
                
                summary_text += f"""📁 FILE: {os.path.basename(filepath)}
   🏆 BEST STRATEGY (Orders): {best_orders['sorting_strategy']}
      → {best_orders['metrics']['releasable_count']:>6}/{best_orders['metrics']['total_orders']:>6} orders releasable ({best_orders['metrics']['releasable_count']/best_orders['metrics']['total_orders']*100:.1f}%)
   
   🏆 BEST STRATEGY (Hours): {best_hours['sorting_strategy']}
      → {best_hours['metrics']['releasable_hours']:,.0f}/{best_hours['metrics']['total_hours']:,.0f} hours releasable ({best_hours['metrics']['releasable_hours']/best_hours['metrics']['total_hours']*100:.1f}%)
   
   🏆 BEST STRATEGY (Qty): {best_qty['sorting_strategy']}
      → {best_qty['metrics']['releasable_qty']:,}/{best_qty['metrics']['total_qty']:,} units releasable ({best_qty['metrics']['releasable_qty']/best_qty['metrics']['total_qty']*100:.1f}%)
   
   📉 WORST STRATEGY: {worst_orders['sorting_strategy']}
      → {worst_orders['metrics']['releasable_count']:,} orders releasable
   
   🔺 IMPROVEMENT POTENTIAL: +{improvement_orders:,} more orders ({improvement_pct:.1f}% boost)

"""
            
            summary_text += f"""⏱️ PERFORMANCE METRICS:
   Total Processing Time: {processing_time:.2f} seconds
   Processing Speed: {sum(s['metrics']['total_orders'] for s in scenarios_for_comparison)/processing_time:.1f} orders/second
   Average per Strategy: {processing_time/len(scenarios_for_comparison):.1f} seconds
   
💾 Results saved to: {'No export (Quick Analysis Mode)' if self.no_export_checkbox.isChecked() else 'Desktop'}
   
🔥 OPTIMIZATION FEATURES:
   ✓ All sorting strategies tested
   ✓ Triple optimization: Orders + Hours + Quantity
   ✓ Only optimal results saved as individual sheets
   ✓ Complete strategy comparison table
   ✓ Improvement potential analysis"""
            
        elif len(scenarios) > 1:
            # Multi-scenario summary (standard mode)
            best_scenario = max(scenarios, key=lambda s: s['metrics']['releasable_count'])
            worst_scenario = min(scenarios, key=lambda s: s['metrics']['releasable_count'])
            improvement = best_scenario['metrics']['releasable_count'] - worst_scenario['metrics']['releasable_count']
            
            summary_text = f"""✅ MULTI-SCENARIO ANALYSIS COMPLETE!

📊 SCENARIOS COMPARED: {len(scenarios)}

🔧 MATERIAL CATEGORIES PROCESSED:
   Kits (3001, 3801, 5001): {'✓ Included' if self.include_kits_checkbox.isChecked() else '✗ Excluded'}
   Instruments (3802, 3803, 3804, 3805): {'✓ Included' if self.include_instruments_checkbox.isChecked() else '✗ Excluded'}
   Virtuoso (3806): {'✓ Included' if self.include_virtuoso_checkbox.isChecked() else '✗ Excluded'}
   Kit Samples (KIT SAMPLES): {'✓ Included' if self.include_kit_samples_checkbox.isChecked() else '✗ Excluded'}

🏆 BEST PERFORMER: {os.path.basename(best_scenario['filepath'])}
   ✅ {best_scenario['metrics']['releasable_count']:,} releasable orders ({best_scenario['metrics']['releasable_count']/best_scenario['metrics']['total_orders']*100:.1f}%)

📉 BASELINE: {os.path.basename(worst_scenario['filepath'])}
   ✅ {worst_scenario['metrics']['releasable_count']:,} releasable orders ({worst_scenario['metrics']['releasable_count']/worst_scenario['metrics']['total_orders']*100:.1f}%)

🔺 IMPROVEMENT: +{improvement:,} more orders releasable

⏱️ PERFORMANCE METRICS:
   Total Processing Time: {processing_time:.2f} seconds
   Processing Speed: {sum(s['metrics']['total_orders'] for s in scenarios)/processing_time:.1f} orders/second
   
💾 Results saved to: {'No export (Quick Analysis Mode)' if self.no_export_checkbox.isChecked() else 'Desktop'}"""
        else:
            # Single scenario summary
            scenario = scenarios[0]
            metrics = scenario['metrics']
            
            summary_text = f"""✅ PROCESSING COMPLETE!

📊 RESULTS SUMMARY:
   Total Orders:     {format_metric(safe_metric(metrics, 'total_orders')):>8}
   ✅ Releasable:    {format_metric(safe_metric(metrics, 'releasable_count')):>8} ({format_metric(safe_metric(metrics, 'releasable_count') / safe_metric(metrics, 'total_orders') * 100, 'percentage')})
   ❌ On Hold:       {format_metric(safe_metric(metrics, 'held_count')):>8} ({format_metric(safe_metric(metrics, 'held_count') / safe_metric(metrics, 'total_orders') * 100, 'percentage')})
   🏷️ Piggyback:     {format_metric(safe_metric(metrics, 'pb_count')):>8}
   ⚠️ Skipped:       {format_metric(safe_metric(metrics, 'skipped_count')):>8}

🔧 MATERIAL CATEGORIES PROCESSED:
   Kits (3001, 3801, 5001): {'✓ Included' if self.include_kits_checkbox.isChecked() else '✗ Excluded'}
   Instruments (3802, 3803, 3804, 3805): {'✓ Included' if self.include_instruments_checkbox.isChecked() else '✗ Excluded'}
   Virtuoso (3806): {'✓ Included' if self.include_virtuoso_checkbox.isChecked() else '✗ Excluded'}
   Kit Samples (KIT SAMPLES): {'✓ Included' if self.include_kit_samples_checkbox.isChecked() else '✗ Excluded'}

🔧 RELEASABLE KITS:
   BVI Kits (3001, 3801):    {format_metric(safe_metric(metrics, 'releasable_bvi_kits_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_bvi_kits_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_bvi_kits_qty')):>8} qty
   Malosa Kits (5001):       {format_metric(safe_metric(metrics, 'releasable_malosa_kits_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_malosa_kits_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_malosa_kits_qty')):>8} qty
   Total Kits:               {format_metric(safe_metric(metrics, 'releasable_kits_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_kits_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_kits_qty')):>8} qty

🔬 RELEASABLE INSTRUMENTS:
   Manufacturing (3802):     {format_metric(safe_metric(metrics, 'releasable_manufacturing_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_manufacturing_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_manufacturing_qty')):>8} qty
   Assembly (3803):          {format_metric(safe_metric(metrics, 'releasable_assembly_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_assembly_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_assembly_qty')):>8} qty
   Packaging (3804):         {format_metric(safe_metric(metrics, 'releasable_packaging_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_packaging_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_packaging_qty')):>8} qty
   Malosa Instruments (3805):{format_metric(safe_metric(metrics, 'releasable_malosa_instruments_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_malosa_instruments_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_malosa_instruments_qty')):>8} qty
   Total Instruments:        {format_metric(safe_metric(metrics, 'releasable_instruments_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_instruments_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_instruments_qty')):>8} qty

🎵 RELEASABLE VIRTUOSO:
   Virtuoso (3806):          {format_metric(safe_metric(metrics, 'releasable_virtuoso_count')):>6} orders,  {format_metric(safe_metric(metrics, 'releasable_virtuoso_hours'), 'hours'):>8} hrs,  {format_metric(safe_metric(metrics, 'releasable_virtuoso_qty')):>8} qty

⏱️ LABOR HOURS SUMMARY:
   Total Hours:              {format_metric(safe_metric(metrics, 'total_hours'), 'hours'):>8}
   ✅ Releasable Hours:       {format_metric(safe_metric(metrics, 'releasable_hours'), 'hours'):>8} ({format_metric(safe_metric(metrics, 'releasable_hours') / safe_metric(metrics, 'total_hours') * 100, 'percentage')})

⏱️ PERFORMANCE METRICS:
   Processing Time: {processing_time:.2f} seconds
   Orders per Second: {metrics['total_orders']/processing_time:.1f}

💾 Results saved to: {'No export (Quick Analysis Mode)' if self.no_export_checkbox.isChecked() else 'Desktop'}"""
        
        # Add performance report to the summary
        performance_report = self.generate_performance_report()
        full_summary = summary_text + "\n\n" + performance_report
        
        # Display results
        self.results_text.setPlainText(full_summary)
        
        # Update status
        if minmax_mode:
            self.status_label.setText(f"🔥 MIN/MAX OPTIMIZATION COMPLETE! {len(scenarios_for_comparison)} strategies tested, {len(scenarios)} best results saved in {processing_time:.1f}s")
        elif len(scenarios) > 1:
            best_scenario = max(scenarios, key=lambda s: s['metrics']['releasable_count'])
            worst_scenario = min(scenarios, key=lambda s: s['metrics']['releasable_count'])
            improvement = best_scenario['metrics']['releasable_count'] - worst_scenario['metrics']['releasable_count']
            self.status_label.setText(f"✅ ALL {len(scenarios)} SCENARIOS COMPLETE! Best: {best_scenario['metrics']['releasable_count']:,} releasable (+{improvement:,} vs worst) | Total time: {processing_time:.1f}s")
        else:
            total_orders = scenarios[0]['metrics']['total_orders']
            total_releasable = scenarios[0]['metrics']['releasable_count']
            self.status_label.setText(f"✅ PROCESSING COMPLETE! {total_releasable:,}/{total_orders:,} orders releasable in {processing_time:.1f}s")
    
    def generate_performance_report(self):
        """Generate detailed performance report with phase breakdown"""
        phase_summary = performance_tracker.get_phase_summary()
        memory_summary = performance_tracker.get_memory_summary()
        
        report = []
        report.append("🔍 DETAILED PERFORMANCE ANALYSIS")
        report.append("=" * 50)
        
        # Phase breakdown
        report.append("\n📊 PHASE BREAKDOWN:")
        total_time = 0
        for phase, metrics in phase_summary.items():
            total_time += metrics['total_time']
            report.append(f"   {phase}:")
            report.append(f"     Total Time: {metrics['total_time']:.3f}s")
            report.append(f"     Average Time: {metrics['avg_time']:.3f}s")
            report.append(f"     Count: {metrics['count']}")
            report.append(f"     Min/Max: {metrics['min_time']:.3f}s / {metrics['max_time']:.3f}s")
        
        # Calculate percentages
        report.append(f"\n📈 TIME DISTRIBUTION:")
        for phase, metrics in phase_summary.items():
            percentage = (metrics['total_time'] / total_time * 100) if total_time > 0 else 0
            report.append(f"   {phase}: {percentage:.1f}% ({metrics['total_time']:.3f}s)")
        
        # Memory usage
        if memory_summary:
            report.append(f"\n💾 MEMORY USAGE:")
            report.append(f"   Peak Memory: {memory_summary['peak_memory_mb']:.1f} MB")
            report.append(f"   Average Memory: {memory_summary['avg_memory_mb']:.1f} MB")
            report.append(f"   Initial Memory: {memory_summary['initial_memory_mb']:.1f} MB")
            report.append(f"   Final Memory: {memory_summary['final_memory_mb']:.1f} MB")
            report.append(f"   Memory Growth: {memory_summary['final_memory_mb'] - memory_summary['initial_memory_mb']:.1f} MB")
        
        return "\n".join(report)
    
    def copy_summary_to_clipboard(self):
        """Copy the summary text to clipboard"""
        summary_text_content = self.results_text.toPlainText().strip()
        clipboard = QApplication.clipboard()
        clipboard.setText(summary_text_content)
        
        # Temporarily change button text to show it was copied
        original_text = self.copy_btn.text()
        self.copy_btn.setText("✅ Copied!")
        QTimer.singleShot(2000, lambda: self.copy_btn.setText(original_text))
    
    def download_quick_analysis_file(self):
        """Download the quick analysis Excel file"""
        if self.quick_analysis_excel_buffer is None:
            QMessageBox.critical(self, "No file", "No quick analysis file available.")
            return
        
        try:
            # Check if buffer has data
            if self.quick_analysis_excel_buffer.getvalue() == b'':
                QMessageBox.critical(self, "No data", "Quick analysis buffer is empty.")
                return
                
            # Try to get a default filename
            default_filename = f"Quick_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Quick Analysis Results",
                default_filename,
                "Excel files (*.xlsx)"
            )
            
            if file_path:
                # Ensure the buffer is at the beginning
                self.quick_analysis_excel_buffer.seek(0)
                
                # Get the buffer data
                buffer_data = self.quick_analysis_excel_buffer.getvalue()
                
                # Check if we have data to write
                if not buffer_data:
                    QMessageBox.critical(self, "Error", "No data available to save.")
                    return
                
                # Write the file with proper error handling
                with open(file_path, "wb") as f:
                    f.write(buffer_data)
                
                QMessageBox.information(self, "Success", f"File saved successfully to:\n{file_path}")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save file:\n{str(e)}")

# Main application
def main():
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    # Create and show the main window
    window = PlanSnapMainWindow()
    window.show()
    
    # Start the event loop
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 