import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import time
import os

# VERSION INFO
VERSION = "v1.1.0"
VERSION_DATE = "2025-01-27"
VERSION_NOTES = "Multi-Department Scheduling - Manufacturing, Assembly, Packaging, Malosa"

@dataclass
class DepartmentOrder:
    """Represents an order for department scheduling"""
    order_no: str
    part_no: str
    quantity: int
    start_date: datetime
    due_date: datetime
    planner: str = ""
    brand: str = ""
    format: str = ""
    area: str = ""    # Department assignment
    hours: float = 0.0
    priority_score: float = 0.0
    scheduled_date: Optional[datetime] = None
    is_medipack: bool = False
    
@dataclass
class Department:
    """Represents a department with scheduling capacity"""
    name: str
    available_hours: float
    used_hours: float = 0.0
    daily_hours: float = 8.0  # Typical workday hours
    
    def remaining_hours(self) -> float:
        return max(0, self.available_hours - self.used_hours)
    
    def can_accommodate_hours(self, required_hours: float) -> bool:
        return self.remaining_hours() >= required_hours
    
    def utilization_pct(self) -> float:
        return (self.used_hours / self.available_hours * 100) if self.available_hours > 0 else 0
    
    def estimated_days(self) -> float:
        """Estimate how many working days this represents"""
        return self.available_hours / self.daily_hours if self.daily_hours > 0 else 0

class MultiDepartmentScheduler:
    """Scheduler that handles multiple independent departments"""
    
    def __init__(self):
        self.departments: Dict[str, Department] = {}
        self.orders_by_dept: Dict[str, List[DepartmentOrder]] = {}
        self.scheduled_orders_by_dept: Dict[str, List[DepartmentOrder]] = {}
        self.unscheduled_orders_by_dept: Dict[str, List[DepartmentOrder]] = {}
        self.hours_lookup: Dict[str, float] = {}  # Part number to hours per unit
        
    def add_department(self, name: str, available_hours: float, daily_hours: float = 8.0):
        """Add a department with scheduling capacity"""
        self.departments[name] = Department(name, available_hours, daily_hours=daily_hours)
        
        # Only initialize orders lists if they don't exist yet
        if name not in self.orders_by_dept:
            self.orders_by_dept[name] = []
        if name not in self.scheduled_orders_by_dept:
            self.scheduled_orders_by_dept[name] = []
        if name not in self.unscheduled_orders_by_dept:
            self.unscheduled_orders_by_dept[name] = []
    
    def load_hours_data(self, df_hours: pd.DataFrame):
        """Load hours per unit data for parts"""
        self.hours_lookup = {}
        
        if 'PART_NO' in df_hours.columns and 'Hours per Unit' in df_hours.columns:
            # Group by part number and sum hours (in case of multiple operations per part)
            # Convert part numbers to strings for consistent lookup
            df_hours_clean = df_hours.copy()
            df_hours_clean['PART_NO'] = df_hours_clean['PART_NO'].astype(str)
            
            hours_summary = df_hours_clean.groupby('PART_NO')['Hours per Unit'].sum()
            self.hours_lookup = hours_summary.to_dict()
            
            # Also add integer versions if possible (for backward compatibility)
            for part_str, hours in list(self.hours_lookup.items()):
                try:
                    part_int = int(float(part_str))
                    if part_int not in self.hours_lookup:  # Don't overwrite existing
                        self.hours_lookup[part_int] = hours
                except (ValueError, TypeError):
                    pass  # Skip if can't convert to int
    
    def calculate_order_hours(self, order: DepartmentOrder):
        """Calculate required hours for this order with robust part number matching"""
        # Try multiple data type formats to find hours
        hours_per_unit = 0.0
        
        # Try as string first
        hours_per_unit = self.hours_lookup.get(str(order.part_no), 0.0)
        
        # If not found, try as integer
        if hours_per_unit == 0.0:
            try:
                part_as_int = int(float(order.part_no))
                hours_per_unit = self.hours_lookup.get(part_as_int, 0.0)
            except (ValueError, TypeError):
                pass
        
        # If still not found, try the original part number as-is
        if hours_per_unit == 0.0:
            hours_per_unit = self.hours_lookup.get(order.part_no, 0.0)
        
        order.hours = hours_per_unit * order.quantity
        
        # For debugging - these aren't used in the current logic but keep for future
        order.manufacturing_hours = 0.0
        order.assembly_hours = 0.0  
        order.packaging_hours = 0.0
        order.total_hours = order.hours
    
    def assign_order_to_department(self, order: DepartmentOrder):
        """Assign order to appropriate department and calculate hours"""
        # Calculate required hours first
        self.calculate_order_hours(order)
        
        # Check if this is a Medipack order (for Packaging department)
        if order.area.lower() == 'packaging' and order.format.lower() == 'medipack':
            order.is_medipack = True
        
        # Assign to department
        dept_name = order.area.lower()
        if dept_name in self.orders_by_dept:
            self.orders_by_dept[dept_name].append(order)
        else:
            # Create department if it doesn't exist
            self.add_department(dept_name, 1000.0)  # Default capacity
            self.orders_by_dept[dept_name].append(order)
    
    def calculate_order_priority(self, order: DepartmentOrder, current_date: datetime) -> float:
        """Calculate priority score with age preference"""
        # Age factor - older orders get higher priority
        days_old = (current_date - order.start_date).days
        age_score = min(days_old * 0.2, 10.0)  # Cap at 10.0
        
        # Due date urgency
        days_to_due = (order.due_date - current_date).days
        urgency_score = max(0, 15.0 - days_to_due * 0.3)
        
        # Medipack priority boost
        medipack_score = 5.0 if order.is_medipack else 0.0
        
        # Efficiency factor based on order size
        efficiency_score = min(order.quantity / 200.0, 2.0)  # Normalize to typical order size
        
        # Combined priority (age most important, then medipack, then urgency)
        total_score = age_score * 3.0 + medipack_score + urgency_score + efficiency_score
        
        return total_score
    
    def check_operation_capacity(self, order: DepartmentOrder, department: Department) -> Tuple[bool, List[str]]:
        """Check if department has sufficient capacity for this order"""
        violations = []
        
        if order.hours > 0:
            if not department.can_accommodate_hours(order.hours):
                available = department.remaining_hours()
                violations.append(f"{department.name}: need {order.hours:.1f}h, available {available:.1f}h")
        
        return len(violations) == 0, violations
    
    def schedule_packaging_department(self, dept_name: str, target_date: datetime) -> Dict[str, Any]:
        """Special scheduling logic for Packaging with Medipack priority and format batching"""
        orders = self.orders_by_dept[dept_name]
        department = self.departments[dept_name]
        
        print(f"\n--- Packaging Scheduling for {dept_name} ---")
        print(f"Orders to schedule: {len(orders)}")
        print(f"Daily target: {department.daily_hours} hours/day (split: 7.5h Medipack + 54.7h others)")
        
        if not orders:
            return {'scheduled': [], 'unscheduled': [], 'daily_schedule': {}}
        
        # Calculate priorities
        for order in orders:
            order.priority_score = self.calculate_order_priority(order, target_date)
        
        # Separate Medipack orders (highest priority)
        medipack_orders = [order for order in orders if order.is_medipack and order.hours > 0]
        other_orders = [order for order in orders if not order.is_medipack and order.hours > 0]
        zero_hours_orders = [order for order in orders if order.hours <= 0]
        
        # Sort both groups by priority
        medipack_orders.sort(key=lambda x: (-x.priority_score, x.start_date))
        other_orders.sort(key=lambda x: (-x.priority_score, x.start_date))
        
        scheduled = []
        unscheduled = zero_hours_orders  # Orders with no hours data
        daily_schedule = {}
        current_date = target_date
        
        # Medipack capacity: 7.5 hours/day
        medipack_daily_target = 7.5
        medipack_daily_used = 0
        
        # Other orders capacity: 54.7 hours/day  
        other_daily_target = 54.7
        other_daily_used = 0
        
        print(f"Medipack orders to schedule: {len(medipack_orders)}")
        print(f"Other orders to schedule: {len(other_orders)}")
        
        # Phase 1: Schedule all Medipack orders first (7.5 hours/day target)
        for order in medipack_orders:
            # Check if we need to move to next day for Medipack
            if medipack_daily_used + order.hours > medipack_daily_target:
                current_date += timedelta(days=1)
                medipack_daily_used = 0
                other_daily_used = 0  # Reset other daily counter too
            
            order.scheduled_date = current_date
            scheduled.append(order)
            medipack_daily_used += order.hours
            department.used_hours += order.hours
            
            # Track daily schedule
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str not in daily_schedule:
                daily_schedule[date_str] = {'orders': [], 'hours': 0, 'formats': set(), 'medipack_hours': 0, 'other_hours': 0}
            daily_schedule[date_str]['orders'].append(order)
            daily_schedule[date_str]['hours'] += order.hours
            daily_schedule[date_str]['medipack_hours'] += order.hours
            daily_schedule[date_str]['formats'].add('Medipack')
        
        # Phase 2: Schedule other orders with format batching (54.7 hours/day, 2 formats/day)
        format_groups = {}
        for order in other_orders:
            fmt = order.format or 'Unknown'
            if fmt not in format_groups:
                format_groups[fmt] = []
            format_groups[fmt].append(order)
        
        # Sort format groups by total priority
        sorted_formats = sorted(format_groups.items(), 
                              key=lambda x: sum(order.priority_score for order in x[1]), 
                              reverse=True)
        
        # Get current day info
        current_date_str = current_date.strftime('%Y-%m-%d')
        current_day_formats = daily_schedule.get(current_date_str, {}).get('formats', set())
        
        for format_name, format_orders in sorted_formats:
            for order in format_orders:
                # Check if we can add this format to current day (max 2 formats + Medipack)
                can_fit_today = (
                    other_daily_used + order.hours <= other_daily_target and
                    (len(current_day_formats) < 3 or format_name in current_day_formats)  # Allow Medipack + 2 others
                )
                
                if not can_fit_today:
                    # Move to next day
                    current_date += timedelta(days=1)
                    other_daily_used = 0
                    medipack_daily_used = 0
                    current_day_formats = set()
                
                order.scheduled_date = current_date
                scheduled.append(order)
                other_daily_used += order.hours
                department.used_hours += order.hours
                
                # Update daily tracking
                date_str = current_date.strftime('%Y-%m-%d')
                if date_str not in daily_schedule:
                    daily_schedule[date_str] = {'orders': [], 'hours': 0, 'formats': set(), 'medipack_hours': 0, 'other_hours': 0}
                daily_schedule[date_str]['orders'].append(order)
                daily_schedule[date_str]['hours'] += order.hours
                daily_schedule[date_str]['other_hours'] += order.hours
                daily_schedule[date_str]['formats'].add(format_name)
                current_day_formats.add(format_name)
        
        days_needed = (current_date - target_date).days + 1 if scheduled else 0
        total_hours_scheduled = sum(order.hours for order in scheduled)
        
        print(f"Final results: {len(scheduled)} scheduled, {len(unscheduled)} unscheduled")
        print(f"Days needed: {days_needed} days")
        print(f"Total hours: {total_hours_scheduled:.1f}")
        
        return {
            'scheduled': scheduled,
            'unscheduled': unscheduled,
            'daily_schedule': daily_schedule,
            'days_needed': days_needed,
            'total_hours': total_hours_scheduled
        }
    
    def schedule_standard_department(self, dept_name: str, target_date: datetime) -> Dict[str, Any]:
        """Standard scheduling logic - schedule ALL orders with hours > 0"""
        orders = self.orders_by_dept[dept_name]
        department = self.departments[dept_name]
        
        print(f"\n--- Standard Scheduling for {dept_name} ---")
        print(f"Orders to schedule: {len(orders)}")
        print(f"Daily target: {department.daily_hours} hours/day")
        
        if not orders:
            return {'scheduled': [], 'unscheduled': []}
        
        # Calculate priorities and sort
        for order in orders:
            order.priority_score = self.calculate_order_priority(order, target_date)
        
        sorted_orders = sorted(orders, key=lambda x: (-x.priority_score, x.start_date))
        
        # Check first few orders for debugging
        print("Sample orders with hours:")
        for i, order in enumerate(sorted_orders[:3]):
            print(f"  Order {order.order_no}: {order.hours:.3f} hours (priority: {order.priority_score:.2f})")
        
        scheduled = []
        unscheduled = []
        current_date = target_date
        daily_hours_used = 0
        daily_hours_target = department.daily_hours
        total_hours_scheduled = 0
        
        print(f"Starting scheduling with {daily_hours_target} hours/day target")
        
        for order in sorted_orders:
            # Schedule ALL orders that have hours > 0
            if order.hours > 0:
                # Check if we need to move to next day
                if daily_hours_used + order.hours > daily_hours_target:
                    current_date += timedelta(days=1)
                    daily_hours_used = 0
                
                order.scheduled_date = current_date
                scheduled.append(order)
                daily_hours_used += order.hours
                total_hours_scheduled += order.hours
                department.used_hours += order.hours
            else:
                # Only unscheduled if no hours data available
                unscheduled.append(order)
                print(f"Order {order.order_no} unscheduled: zero hours (no hours data)")
        
        days_needed = (current_date - target_date).days + 1 if scheduled else 0
        
        print(f"Final results: {len(scheduled)} scheduled, {len(unscheduled)} unscheduled")
        print(f"Total hours scheduled: {total_hours_scheduled:.1f}")
        print(f"Days needed: {days_needed} days ({total_hours_scheduled/daily_hours_target:.1f} theoretical days)")
        
        return {
            'scheduled': scheduled,
            'unscheduled': unscheduled,
            'days_needed': days_needed,
            'total_hours': total_hours_scheduled
        }
    
    def schedule_all_departments(self, target_date: datetime) -> Dict[str, Any]:
        """Schedule all departments with their specific logic"""
        start_time = time.time()
        
        # Reset all departments
        for dept in self.departments.values():
            dept.used_hours = 0.0
        
        results_by_dept = {}
        
        print(f"\n=== SCHEDULING DEBUG ===")
        print(f"Target date: {target_date}")
        print(f"Departments to process: {list(self.departments.keys())}")
        print(f"Departments with orders: {list(self.orders_by_dept.keys())}")
        
        for dept_name in self.departments.keys():
            print(f"\nProcessing department: {dept_name}")
            orders_count = len(self.orders_by_dept.get(dept_name, []))
            print(f"Orders in this department: {orders_count}")
            
            if orders_count == 0:
                print(f"Skipping {dept_name} - no orders")
                self.scheduled_orders_by_dept[dept_name] = []
                self.unscheduled_orders_by_dept[dept_name] = []
                continue
            
            if dept_name == 'packaging':
                # Special packaging logic with format batching
                print(f"Using packaging logic for {dept_name}")
                dept_results = self.schedule_packaging_department(dept_name, target_date)
            else:
                # Standard scheduling for other departments
                print(f"Using standard logic for {dept_name}")
                dept_results = self.schedule_standard_department(dept_name, target_date)
            
            print(f"Results for {dept_name}: {len(dept_results['scheduled'])} scheduled, {len(dept_results['unscheduled'])} unscheduled")
            
            # Store results
            self.scheduled_orders_by_dept[dept_name] = dept_results['scheduled']
            self.unscheduled_orders_by_dept[dept_name] = dept_results['unscheduled']
            results_by_dept[dept_name] = dept_results
        
        # Calculate overall summary
        total_orders = sum(len(orders) for orders in self.orders_by_dept.values())
        total_scheduled = sum(len(scheduled) for scheduled in self.scheduled_orders_by_dept.values())
        total_unscheduled = sum(len(unscheduled) for unscheduled in self.unscheduled_orders_by_dept.values())
        
        total_quantity = sum(order.quantity for orders in self.orders_by_dept.values() for order in orders)
        scheduled_quantity = sum(order.quantity for scheduled in self.scheduled_orders_by_dept.values() for order in scheduled)
        
        total_hours = sum(order.hours for orders in self.orders_by_dept.values() for order in orders)
        scheduled_hours = sum(order.hours for scheduled in self.scheduled_orders_by_dept.values() for order in scheduled)
        
        processing_time = time.time() - start_time
        
        # Department utilization 
        dept_utilization = {}
        for name, dept in self.departments.items():
            scheduled_orders = len(self.scheduled_orders_by_dept.get(name, []))
            dept_results = results_by_dept.get(name, {})
            days_needed = dept_results.get('days_needed', 0)
            total_dept_hours = dept_results.get('total_hours', 0)
            
            dept_utilization[name] = {
                'daily_target': dept.daily_hours,
                'total_hours_scheduled': total_dept_hours,
                'days_needed': days_needed,
                'avg_daily_utilization': (total_dept_hours / days_needed) if days_needed > 0 else 0,
                'orders_scheduled': scheduled_orders,
                'completion_date': (target_date + timedelta(days=days_needed-1)).strftime('%Y-%m-%d') if days_needed > 0 else 'N/A'
            }
        
        return {
            'summary': {
                'total_orders': total_orders,
                'scheduled_orders': total_scheduled,
                'unscheduled_orders': total_unscheduled,
                'schedule_rate_pct': (total_scheduled / total_orders * 100) if total_orders > 0 else 0,
                'total_quantity': total_quantity,
                'scheduled_quantity': scheduled_quantity,
                'total_hours': total_hours,
                'scheduled_hours': scheduled_hours,
                'processing_time': processing_time
            },
            'department_utilization': dept_utilization,
            'department_results': results_by_dept
        }

class MultiDepartmentSchedulingGUI:
    """User interface for multi-department scheduling"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.scheduler = MultiDepartmentScheduler()
        self.results = None
        
        self.setup_gui()
    
    def setup_gui(self):
        """Initialize the GUI components"""
        self.root.title(f"Multi-Department Scheduling Engine {VERSION}")
        self.root.geometry("1000x800")
        
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title_label = ttk.Label(main_frame, text=f"🏭 Multi-Department Scheduling Engine {VERSION}", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=4, pady=(0, 20))
        
        # File selection section
        file_frame = ttk.LabelFrame(main_frame, text="📁 Data Input", padding="10")
        file_frame.grid(row=1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))
        
        self.file_path_var = tk.StringVar()
        ttk.Label(file_frame, text="Excel File:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.file_path_var, width=70).grid(row=0, column=1, padx=(5, 5))
        ttk.Button(file_frame, text="Browse", command=self.browse_file).grid(row=0, column=2)
        
        # Target date selection
        ttk.Label(file_frame, text="Start Date:").grid(row=1, column=0, sticky=tk.W, pady=(10, 0))
        self.target_date_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(file_frame, textvariable=self.target_date_var, width=20).grid(row=1, column=1, sticky=tk.W, padx=(5, 5), pady=(10, 0))
        
        # Department capacity configuration
        departments_frame = ttk.LabelFrame(main_frame, text="⚙️ Department Capacity (Hours)", padding="10")
        departments_frame.grid(row=2, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Department capacity input fields
        self.dept_vars = {}
        departments = [
            ("Manufacturing", "manufacturing", 1000.0),
            ("Assembly", "assembly", 800.0),
            ("Packaging", "packaging", 1200.0),
            ("Malosa", "malosa", 600.0)
        ]
        
        for i, (label, key, default) in enumerate(departments):
            row = i // 2
            col = (i % 2) * 2
            
            ttk.Label(departments_frame, text=f"{label}:").grid(row=row, column=col, sticky=tk.W, padx=(0, 5))
            var = tk.StringVar(value=str(default))
            self.dept_vars[key] = var
            ttk.Entry(departments_frame, textvariable=var, width=15).grid(row=row, column=col+1, padx=(0, 30))
        
        # Control buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=3, column=0, columnspan=4, pady=20)
        
        ttk.Button(button_frame, text="🔄 Load Data", command=self.load_data).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(button_frame, text="🚀 Create Schedule", command=self.create_schedule).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(button_frame, text="💾 Export Results", command=self.export_results).pack(side=tk.LEFT)
        
        # Results display
        results_frame = ttk.LabelFrame(main_frame, text="📊 Results", padding="10")
        results_frame.grid(row=4, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Results text area with scrollbar
        text_frame = ttk.Frame(results_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        self.results_text = tk.Text(text_frame, height=25, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.results_text.yview)
        self.results_text.configure(yscrollcommand=scrollbar.set)
        
        self.results_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready - Load your multi-department planning data to begin")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=5, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=(10, 0))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(4, weight=1)
    
    def browse_file(self):
        """Browse for Excel file"""
        file_path = filedialog.askopenfilename(
            title="Select Instruments Planning Template",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if file_path:
            self.file_path_var.set(file_path)
    
    def update_departments(self):
        """Update scheduler departments from GUI inputs - DON'T clear orders!"""
        # Only clear and recreate the departments, keep the orders
        old_departments = self.scheduler.departments.copy()
        
        self.scheduler.departments.clear()
        # DON'T clear orders_by_dept - that would lose all loaded data!
        # self.scheduler.orders_by_dept.clear()  # <-- This was the bug!
        
        # Initialize empty scheduled/unscheduled for new run
        self.scheduler.scheduled_orders_by_dept.clear()
        self.scheduler.unscheduled_orders_by_dept.clear()
        
        for key, var in self.dept_vars.items():
            try:
                hours = float(var.get())
                self.scheduler.add_department(key, hours)
            except ValueError:
                messagebox.showerror("Error", f"Invalid capacity value for {key}")
                return False
        
        return True
    
    def safe_int(self, value, default=0):
        """Safely convert value to int with NaN handling"""
        try:
            if pd.isna(value):
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default
    
    def safe_float(self, value, default=0.0):
        """Safely convert value to float with NaN handling"""
        try:
            if pd.isna(value):
                return default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def safe_str(self, value, default=''):
        """Safely convert value to string with NaN handling"""
        try:
            if pd.isna(value):
                return default
            return str(value).strip()
        except (ValueError, TypeError):
            return default
    
    def load_capacity_from_main_sheet(self, filepath):
        """Load capacity limits from the Main sheet header"""
        try:
            # Read first few rows of Main sheet to get capacity limits
            df_main_header = pd.read_excel(filepath, sheet_name="Main", nrows=5)
            
            # Look for limit row (usually row 2, index 1)
            if len(df_main_header) >= 2:
                limit_row = df_main_header.iloc[1]  # Second row typically contains limits
                
                # Try to extract capacity values from the limit row
                # This might need adjustment based on actual file structure
                for col_name in limit_row.index:
                    if 'manufactur' in str(col_name).lower():
                        value = self.safe_float(limit_row[col_name])
                        if value > 0:
                            self.dept_vars['manufacturing'].set(str(value))
                    elif 'assembly' in str(col_name).lower():
                        value = self.safe_float(limit_row[col_name])
                        if value > 0:
                            self.dept_vars['assembly'].set(str(value))
                    elif 'packag' in str(col_name).lower():
                        value = self.safe_float(limit_row[col_name])
                        if value > 0:
                            self.dept_vars['packaging'].set(str(value))
                    elif 'malosa' in str(col_name).lower():
                        value = self.safe_float(limit_row[col_name])
                        if value > 0:
                            self.dept_vars['malosa'].set(str(value))
            
        except Exception as e:
            print(f"Could not load capacity from Main sheet: {e}")
    
    def load_data(self):
        """Load orders and supporting data from Excel file"""
        if not self.file_path_var.get():
            messagebox.showerror("Error", "Please select an Excel file first")
            return
        
        try:
            self.status_var.set("Loading multi-department data from Excel...")
            self.root.update()
            
            # Try to load capacity limits from Main sheet
            self.load_capacity_from_main_sheet(self.file_path_var.get())
            
            # Load main order data from ReleasedPOOL sheet
            df_orders = pd.read_excel(self.file_path_var.get(), sheet_name="ReleasedPOOL")
            
            # Load hours data for operation planning
            try:
                df_hours = pd.read_excel(self.file_path_var.get(), sheet_name="Hrs")
                self.scheduler.load_hours_data(df_hours)
            except Exception as e:
                print(f"Warning: Could not load hours data: {e}")
            
            # Try to load Main sheet for additional details (Area, Format, Brand)
            try:
                df_main = pd.read_excel(self.file_path_var.get(), sheet_name="Main", skiprows=6)
                if not df_main.empty:
                    # Merge additional details if available
                    df_orders = df_orders.merge(
                        df_main[['Order No', 'Brand', 'Format', 'Area']].dropna(), 
                        left_on='Order No', right_on='Order No', how='left'
                    )
            except Exception as e:
                print(f"Warning: Could not load Main sheet details: {e}")
            
            # Convert to DepartmentOrder objects and assign to departments
            area_debug = {}  # Track what areas we're seeing
            orders_processed = 0
            orders_assigned = 0
            
            for _, row in df_orders.iterrows():
                try:
                    order = DepartmentOrder(
                        order_no=self.safe_str(row.get('Order No', '')),
                        part_no=self.safe_str(row.get('Part No', '')),
                        quantity=self.safe_int(row.get('Qty', 0)),
                        start_date=pd.to_datetime(row.get('Start Date', datetime.now()), errors='coerce') or datetime.now(),
                        due_date=pd.to_datetime(row.get('Due Date', datetime.now() + timedelta(days=30)), errors='coerce') or (datetime.now() + timedelta(days=30)),
                        planner=self.safe_str(row.get('Planner', '')),
                        brand=self.safe_str(row.get('Brand', '')),
                        format=self.safe_str(row.get('Format', '')),
                        area=self.safe_str(row.get('Area', ''))
                    )
                    
                    orders_processed += 1
                    
                    # Debug: Track what areas we're seeing
                    area_key = order.area if order.area else '(blank)'
                    area_debug[area_key] = area_debug.get(area_key, 0) + 1
                    
                    # Assign to appropriate department
                    if order.area:
                        self.scheduler.assign_order_to_department(order)
                        orders_assigned += 1
                    else:
                        print(f"Order {order.order_no} has no area assigned")
                    
                except Exception as e:
                    print(f"Error processing order {row.get('Order No', 'Unknown')}: {e}")
                    continue
            
            # Debug output
            print(f"\n=== DEBUGGING INFO ===")
            print(f"Orders processed: {orders_processed}")
            print(f"Orders assigned to departments: {orders_assigned}")
            print(f"Areas found in data:")
            for area, count in area_debug.items():
                print(f"  '{area}': {count} orders")
            print(f"Expected departments: {list(self.scheduler.departments.keys())}")
            print(f"Orders by department:")
            for dept_name, orders in self.scheduler.orders_by_dept.items():
                print(f"  {dept_name}: {len(orders)} orders")
            
            # HOURS DEBUGGING
            print(f"\n=== HOURS CALCULATION DEBUG ===")
            print(f"Hours lookup entries loaded: {len(self.scheduler.hours_lookup)}")
            if self.scheduler.hours_lookup:
                print("Sample hours lookup entries:")
                for i, (part, hours) in enumerate(list(self.scheduler.hours_lookup.items())[:5]):
                    print(f"  {part}: {hours} hours/unit")
            
            # Check hours for first few orders in each department
            total_orders_with_hours = 0
            total_orders_zero_hours = 0
            
            for dept_name, orders in self.scheduler.orders_by_dept.items():
                if orders:
                    print(f"\n{dept_name.title()} orders hours check:")
                    for i, order in enumerate(orders[:3]):  # Check first 3 orders
                        hours_per_unit = self.scheduler.hours_lookup.get(order.part_no, 0.0)
                        calculated_hours = hours_per_unit * order.quantity
                        
                        # Enhanced debugging for part number matching
                        print(f"  Order {order.order_no} - Part {order.part_no} (type: {type(order.part_no)}) - Qty {order.quantity}")
                        print(f"    Looking for: '{order.part_no}' in hours lookup...")
                        
                        # Check if part exists in lookup with different data types
                        part_as_str = str(order.part_no)
                        part_as_int = None
                        try:
                            part_as_int = int(float(order.part_no))
                        except:
                            pass
                            
                        str_lookup = self.scheduler.hours_lookup.get(part_as_str, 0.0)
                        int_lookup = self.scheduler.hours_lookup.get(part_as_int, 0.0) if part_as_int else 0.0
                        
                        print(f"    Hours lookup as string '{part_as_str}': {str_lookup}")
                        if part_as_int:
                            print(f"    Hours lookup as int {part_as_int}: {int_lookup}")
                        
                        print(f"    Final hours/unit: {hours_per_unit} - Calculated: {calculated_hours} - Stored: {order.hours}")
                        
                        # Check if this part exists in lookup at all
                        matching_keys = [k for k in self.scheduler.hours_lookup.keys() if str(k) == str(order.part_no)]
                        if matching_keys:
                            print(f"    Found matching keys: {matching_keys}")
                        else:
                            print(f"    No matching keys found for part {order.part_no}")
                        
                        if order.hours > 0:
                            total_orders_with_hours += 1
                        else:
                            total_orders_zero_hours += 1
                        if i >= 2:  # Only show first 3
                            break
            
            print(f"\nOverall hours summary:")
            print(f"Orders with hours > 0: {total_orders_with_hours}")
            print(f"Orders with zero hours: {total_orders_zero_hours}")
            print(f"=== END HOURS DEBUG ===\n")
            
            # DATA QUALITY CHECK: Only Packaging orders with "-" format are problematic
            packaging_format_issues = []
            for dept_name, orders in self.scheduler.orders_by_dept.items():
                if 'packag' in dept_name.lower():  # Only check Packaging department
                    for order in orders:
                        if order.format == '-' or order.format == '':
                            packaging_format_issues.append(order)
            
            # Update results display
            self.results_text.delete(1.0, tk.END)
            self.results_text.insert(tk.END, f"✅ MULTI-DEPARTMENT DATA LOADED SUCCESSFULLY!\n\n")
            self.results_text.insert(tk.END, f"📊 OVERALL SUMMARY:\n")
            
            total_orders = sum(len(orders) for orders in self.scheduler.orders_by_dept.values())
            total_quantity = sum(order.quantity for orders in self.scheduler.orders_by_dept.values() for order in orders)
            
            self.results_text.insert(tk.END, f"Total orders loaded: {total_orders:,}\n")
            self.results_text.insert(tk.END, f"Total quantity: {total_quantity:,}\n")
            self.results_text.insert(tk.END, f"Hours data entries: {len(self.scheduler.hours_lookup):,}\n\n")
            
            # Department breakdown
            self.results_text.insert(tk.END, f"🏭 DEPARTMENT BREAKDOWN:\n")
            for dept_name, orders in self.scheduler.orders_by_dept.items():
                if orders:
                    dept_qty = sum(order.quantity for order in orders)
                    dept_hours = sum(order.hours for order in orders)
                    medipack_count = sum(1 for order in orders if order.is_medipack)
                    
                    self.results_text.insert(tk.END, f"{dept_name.title()}: {len(orders):,} orders, {dept_qty:,} qty, {dept_hours:.1f} hours")
                    if medipack_count > 0:
                        self.results_text.insert(tk.END, f" ({medipack_count} Medipack)")
                    self.results_text.insert(tk.END, f"\n")
            
            # Show format breakdown for Packaging
            if 'packaging' in self.scheduler.orders_by_dept:
                packaging_orders = self.scheduler.orders_by_dept['packaging']
                format_counts = {}
                for order in packaging_orders:
                    fmt = order.format or 'Unknown'
                    format_counts[fmt] = format_counts.get(fmt, 0) + 1
                
                self.results_text.insert(tk.END, f"\n📦 PACKAGING FORMAT BREAKDOWN:\n")
                for fmt, count in sorted(format_counts.items()):
                    self.results_text.insert(tk.END, f"{fmt}: {count:,} orders\n")
            
            # DATA QUALITY WARNINGS - Only for Packaging department
            if packaging_format_issues:
                self.results_text.insert(tk.END, f"\n⚠️ PACKAGING DATA QUALITY ISSUES FOUND!\n")
                self.results_text.insert(tk.END, f"🚨 PACKAGING orders with invalid formats: {len(packaging_format_issues)} orders have '-' or blank format\n")
                self.results_text.insert(tk.END, f"(Note: Manufacturing/Assembly/Malosa orders with '-' format are normal)\n")
                self.results_text.insert(tk.END, f"These PACKAGING orders need format correction:\n")
                
                # Show first 10 problematic orders
                for i, order in enumerate(packaging_format_issues[:10]):
                    self.results_text.insert(tk.END, f"• {order.order_no} - Part: {order.part_no} - Format: '{order.format}'\n")
                
                if len(packaging_format_issues) > 10:
                    self.results_text.insert(tk.END, f"... and {len(packaging_format_issues) - 10} more orders\n")
                
                self.results_text.insert(tk.END, f"\n⚠️ Please correct format data before creating schedule!\n")
                
                # Update status to show warning
                self.status_var.set(f"⚠️ Data loaded with {len(packaging_format_issues)} PACKAGING format issues - Fix before scheduling")
            else:
                self.status_var.set(f"Data loaded: {total_orders:,} orders across {len(self.scheduler.orders_by_dept)} departments")
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data: {str(e)}")
            self.status_var.set("Error loading data")
    
    def create_schedule(self):
        """Create optimized multi-department schedule"""
        if not any(self.scheduler.orders_by_dept.values()):
            messagebox.showerror("Error", "Please load order data first")
            return
        
        if not self.update_departments():
            return
        
        try:
            # Check for data quality issues before scheduling (Packaging format issues only)
            packaging_format_issues = []
            for dept_name, orders in self.scheduler.orders_by_dept.items():
                if 'packag' in dept_name.lower():  # Only Packaging department
                    for order in orders:
                        if order.format == '-' or order.format == '':
                            packaging_format_issues.append(order)
            
            if packaging_format_issues:
                result = messagebox.askyesno(
                    "Packaging Format Warning", 
                    f"Found {len(packaging_format_issues)} PACKAGING orders with invalid formats ('-' or blank).\n\n"
                    f"Packaging orders need proper format values (not '-') for scheduling.\n"
                    f"Manufacturing/Assembly/Malosa orders with '-' format are OK.\n\n"
                    f"Continue with scheduling anyway?"
                )
                if not result:
                    return
            
            self.status_var.set("Creating optimized multi-department schedule...")
            self.root.update()
            
            # Parse target date
            target_date = datetime.strptime(self.target_date_var.get(), "%Y-%m-%d")
            
            # Run scheduling engine
            results = self.scheduler.schedule_all_departments(target_date)
            self.results = results
            
            # Display results
            self.display_results(results)
            
            summary = results['summary']
            self.status_var.set(f"Schedule complete: {summary['scheduled_orders']:,}/{summary['total_orders']:,} orders scheduled ({summary['schedule_rate_pct']:.1f}%)")
            
        except ValueError as e:
            messagebox.showerror("Error", f"Invalid date format. Use YYYY-MM-DD")
        except Exception as e:
            messagebox.showerror("Error", f"Scheduling failed: {str(e)}")
            self.status_var.set("Scheduling failed")
    
    def display_results(self, results):
        """Display scheduling results"""
        self.results_text.delete(1.0, tk.END)
        
        summary = results['summary']
        
        # Header
        self.results_text.insert(tk.END, f"🏭 MULTI-DEPARTMENT SCHEDULE COMPLETE!\n")
        self.results_text.insert(tk.END, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.results_text.insert(tk.END, f"Start Date: {self.target_date_var.get()}\n\n")
        
        # Overall summary metrics
        self.results_text.insert(tk.END, f"📊 OVERALL SUMMARY:\n")
        self.results_text.insert(tk.END, f"✅ Scheduled Orders: {summary['scheduled_orders']:,} ({summary['schedule_rate_pct']:.1f}%)\n")
        self.results_text.insert(tk.END, f"❌ Unscheduled Orders: {summary['unscheduled_orders']:,}\n")
        self.results_text.insert(tk.END, f"📦 Scheduled Quantity: {summary['scheduled_quantity']:,} / {summary['total_quantity']:,}\n")
        self.results_text.insert(tk.END, f"⏱️ Scheduled Hours: {summary['scheduled_hours']:,.1f} / {summary['total_hours']:,.1f}\n")
        self.results_text.insert(tk.END, f"⚡ Processing Time: {summary['processing_time']:.2f} seconds\n\n")
        
        # Department utilization
        self.results_text.insert(tk.END, f"🏭 DEPARTMENT UTILIZATION:\n")
        for name, usage in results['department_utilization'].items():
            self.results_text.insert(tk.END, f"{name.title()}: {usage['used_hours']:.1f} / {usage['available_hours']:.1f} hours ")
            self.results_text.insert(tk.END, f"({usage['utilization_pct']:.1f}% used, ~{usage['estimated_days']:.1f} days)\n")
        
        # Special formatting for Packaging daily schedule
        if 'packaging' in results['department_results'] and 'daily_schedule' in results['department_results']['packaging']:
            daily_schedule = results['department_results']['packaging']['daily_schedule']
            if daily_schedule:
                self.results_text.insert(tk.END, f"\n📦 PACKAGING DAILY SCHEDULE:\n")
                for date_str, day_data in sorted(daily_schedule.items()):
                    formats_str = ', '.join(day_data['formats'])
                    self.results_text.insert(tk.END, f"{date_str}: {len(day_data['orders'])} orders, {day_data['hours']:.1f} hours, Formats: {formats_str}\n")
        
        # Department breakdown
        self.results_text.insert(tk.END, f"\n📋 DEPARTMENT SUMMARY:\n")
        for dept_name, usage in results['department_utilization'].items():
            unscheduled_count = len(self.scheduler.unscheduled_orders_by_dept.get(dept_name, []))
            total_dept_orders = usage['orders_scheduled'] + unscheduled_count
            
            if total_dept_orders > 0:
                rate_pct = (usage['orders_scheduled'] / total_dept_orders * 100) if total_dept_orders > 0 else 0
                self.results_text.insert(tk.END, f"{dept_name.title()}: {usage['orders_scheduled']}/{total_dept_orders} orders scheduled ({rate_pct:.1f}%)\n")
                if unscheduled_count > 0:
                    self.results_text.insert(tk.END, f"  → {unscheduled_count} orders unscheduled (missing hours data)\n")
    
    def export_results(self):
        """Export scheduling results to Excel"""
        if not self.results:
            messagebox.showerror("Error", "No results to export. Create a schedule first.")
            return
        
        try:
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = f"Multi_Department_Schedule_{VERSION}_{timestamp}.xlsx"
            
            # Ask user for save location
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                initialfile=filename
            )
            
            if not file_path:
                return
            
            self.status_var.set("Exporting results...")
            self.root.update()
            
            # Prepare data for export
            all_scheduled_data = []
            all_unscheduled_data = []
            
            # Collect all scheduled orders from all departments
            for dept_name, scheduled_orders in self.scheduler.scheduled_orders_by_dept.items():
                for order in scheduled_orders:
                    all_scheduled_data.append({
                        'Department': dept_name.title(),
                        'Order No': order.order_no,
                        'Part No': order.part_no,
                        'Quantity': order.quantity,
                        'Start Date': order.start_date.strftime('%Y-%m-%d'),
                        'Due Date': order.due_date.strftime('%Y-%m-%d'),
                        'Scheduled Date': order.scheduled_date.strftime('%Y-%m-%d') if order.scheduled_date else '',
                        'Planner': order.planner,
                        'Brand': order.brand,
                        'Format': order.format,
                        'Hours': f"{order.hours:.2f}",
                        'Priority Score': f"{order.priority_score:.2f}",
                        'Is Medipack': 'Yes' if order.is_medipack else 'No'
                    })
            
            # Collect all unscheduled orders from all departments  
            for dept_name, unscheduled_orders in self.scheduler.unscheduled_orders_by_dept.items():
                for order in unscheduled_orders:
                    all_unscheduled_data.append({
                        'Department': dept_name.title(),
                        'Order No': order.order_no,
                        'Part No': order.part_no,
                        'Quantity': order.quantity,
                        'Start Date': order.start_date.strftime('%Y-%m-%d'),
                        'Due Date': order.due_date.strftime('%Y-%m-%d'),
                        'Planner': order.planner,
                        'Brand': order.brand,
                        'Format': order.format,
                        'Hours': f"{order.hours:.2f}",
                        'Priority Score': f"{order.priority_score:.2f}",
                        'Is Medipack': 'Yes' if order.is_medipack else 'No'
                    })
            
            # Create summary data
            summary_data = []
            for key, value in self.results['summary'].items():
                summary_data.append({'Metric': key.replace('_', ' ').title(), 'Value': value})
            
            # Department utilization data
            dept_utilization_data = []
            for name, usage in self.results['department_utilization'].items():
                dept_utilization_data.append({
                    'Department': name.title(),
                    'Available Hours': usage['available_hours'],
                    'Used Hours': f"{usage['used_hours']:.1f}",
                    'Remaining Hours': f"{usage['remaining_hours']:.1f}",
                    'Utilization %': f"{usage['utilization_pct']:.1f}%",
                    'Estimated Days': f"{usage['estimated_days']:.1f}",
                    'Orders Scheduled': usage['orders_count']
                })
            
            # Export to Excel
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                pd.DataFrame(all_scheduled_data).to_excel(writer, sheet_name='Scheduled Orders', index=False)
                pd.DataFrame(all_unscheduled_data).to_excel(writer, sheet_name='Unscheduled Orders', index=False)
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                pd.DataFrame(dept_utilization_data).to_excel(writer, sheet_name='Department Utilization', index=False)
                
                # Add Packaging daily schedule if available
                if ('packaging' in self.results['department_results'] and 
                    'daily_schedule' in self.results['department_results']['packaging']):
                    daily_schedule = self.results['department_results']['packaging']['daily_schedule']
                    daily_schedule_data = []
                    for date_str, day_data in sorted(daily_schedule.items()):
                        daily_schedule_data.append({
                            'Date': date_str,
                            'Orders Count': len(day_data['orders']),
                            'Total Hours': f"{day_data['hours']:.1f}",
                            'Formats': ', '.join(day_data['formats']),
                            'Order Numbers': ', '.join([order.order_no for order in day_data['orders']])
                        })
                    pd.DataFrame(daily_schedule_data).to_excel(writer, sheet_name='Packaging Daily Schedule', index=False)
            
            self.status_var.set(f"Results exported to {os.path.basename(file_path)}")
            messagebox.showinfo("Success", f"Results exported successfully!\n\nFile: {file_path}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Export failed: {str(e)}")
            self.status_var.set("Export failed")
    
    def run(self):
        """Start the GUI application"""
        # Initial message
        self.results_text.insert(tk.END, f"🏭 Multi-Department Scheduling Engine {VERSION}\n")
        self.results_text.insert(tk.END, f"Version: {VERSION} ({VERSION_DATE})\n")
        self.results_text.insert(tk.END, f"Features: {VERSION_NOTES}\n\n")
        
        self.results_text.insert(tk.END, "INSTRUCTIONS:\n")
        self.results_text.insert(tk.END, "1. 📁 Browse and select your Instruments Planning Template.xlsm file\n")
        self.results_text.insert(tk.END, "2. ⚙️ Adjust department capacity (hours) as needed\n")
        self.results_text.insert(tk.END, "3. 🔄 Click 'Load Data' to import orders by department\n")
        self.results_text.insert(tk.END, "4. 🚀 Click 'Create Schedule' to optimize each department\n")
        self.results_text.insert(tk.END, "5. 💾 Export results to Excel with scheduled dates\n\n")
        
        self.results_text.insert(tk.END, "PLANNING APPROACH:\n")
        self.results_text.insert(tk.END, "• Schedules ALL orders with valid hours data (no artificial limits)\n")
        self.results_text.insert(tk.END, "• Uses daily targets to spread work across realistic timelines\n")
        self.results_text.insert(tk.END, "• Different departments can have different completion dates\n")
        self.results_text.insert(tk.END, "• Manufacturing/Assembly/Malosa: Age-priority scheduling\n")
        self.results_text.insert(tk.END, "• Packaging: Medipack priority (7.5h/day) + format batching (54.7h/day)\n")
        self.results_text.insert(tk.END, "• Shows realistic planning timeline with completion dates\n\n")
        
        self.results_text.insert(tk.END, "Ready to begin multi-department scheduling! 🎯\n")
        
        self.root.mainloop()

if __name__ == "__main__":
    app = MultiDepartmentSchedulingGUI()
    app.run()