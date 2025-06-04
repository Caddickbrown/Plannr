import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import time
import os
import threading

# VERSION INFO
VERSION = "v1.0.0"
VERSION_DATE = "2025-01-27"
VERSION_NOTES = "Initial release - Flexible constraint-based scheduling engine"

@dataclass
class Order:
    """Represents a production order to be scheduled"""
    order_no: str
    part_no: str
    quantity: int
    start_date: datetime
    due_date: datetime
    picks: int = 0
    hours: float = 0.0
    boxes: int = 0
    country: str = ""
    brand: str = ""
    wrap_type: str = ""
    cpu: int = 0
    priority_score: float = 0.0
    assigned_line: str = ""
    scheduled_date: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate order data after initialization"""
        if not self.order_no or not self.order_no.strip():
            raise ValueError("Order number cannot be empty")
        if not self.part_no or not self.part_no.strip():
            raise ValueError("Part number cannot be empty")
        if self.quantity <= 0:
            raise ValueError("Quantity must be greater than 0")
        if self.due_date < self.start_date:
            raise ValueError("Due date cannot be before start date")
        if self.picks < 0:
            raise ValueError("Picks cannot be negative")
        if self.hours < 0:
            raise ValueError("Hours cannot be negative")
        if self.boxes < 0:
            raise ValueError("Boxes cannot be negative")
    
@dataclass
class Constraint:
    """Flexible constraint definition"""
    name: str
    limit: float
    current_used: float = 0.0
    constraint_type: str = "capacity"  # capacity, resource, geographic, etc.
    
    def remaining(self) -> float:
        return max(0, self.limit - self.current_used)
    
    def can_accommodate(self, required: float) -> bool:
        return self.remaining() >= required

@dataclass
class LineConfiguration:
    """Production line configuration with staffing options"""
    line_name: str
    total_associates: int
    pick_associates: int
    wrap_associates: int
    seal_associates: int
    line_takt_time: float
    pick_time: float
    wrap_time: float
    seal_time: float
    
    def calculate_throughput(self, order: Order) -> Dict[str, float]:
        """Calculate time requirements for this order on this line"""
        return {
            'pick_time': order.picks * self.pick_time / self.pick_associates,
            'wrap_time': order.quantity * self.wrap_time / self.wrap_associates,
            'seal_time': order.quantity * self.seal_time / self.seal_associates,
            'total_time': max(
                order.picks * self.pick_time / self.pick_associates,
                order.quantity * self.wrap_time / self.wrap_associates,
                order.quantity * self.seal_time / self.seal_associates
            )
        }

class SchedulingEngine:
    """Core scheduling engine with flexible constraint system"""
    
    def __init__(self):
        self.orders: List[Order] = []
        self.constraints: Dict[str, Constraint] = {}
        self.line_configs: Dict[str, LineConfiguration] = {}
        self.scheduled_orders: List[Order] = []
        self.unscheduled_orders: List[Order] = []
        
    def add_constraint(self, name: str, limit: float, constraint_type: str = "capacity"):
        """Add a flexible constraint to the system"""
        self.constraints[name] = Constraint(name, limit, constraint_type=constraint_type)
    
    def add_line_configuration(self, config: LineConfiguration):
        """Add a production line configuration"""
        self.line_configs[config.line_name] = config
    
    def calculate_order_priority(self, order: Order, current_date: datetime) -> float:
        """Calculate priority score balancing age and efficiency"""
        # Age factor - older orders get higher priority
        days_old = (current_date - order.start_date).days
        age_score = min(days_old * 0.1, 5.0)  # Cap at 5.0
        
        # Due date urgency
        days_to_due = (order.due_date - current_date).days
        urgency_score = max(0, 10.0 - days_to_due * 0.2)  # Higher when due soon
        
        # Efficiency factor - larger orders get slight preference
        efficiency_score = min(order.quantity / 100.0, 2.0)  # Cap at 2.0
        
        # Combined priority (age is most important)
        total_score = age_score * 2.0 + urgency_score + efficiency_score
        
        return total_score
    
    def check_constraints(self, order: Order) -> Tuple[bool, List[str]]:
        """Check if order can fit within current constraints"""
        violations = []
        
        # Check each constraint
        for name, constraint in self.constraints.items():
            if constraint.limit < 0:
                violations.append(f"{name}: Invalid negative limit {constraint.limit}")
                continue
                
            try:
                required = self.get_order_constraint_value(order, name)
                if required < 0:
                    violations.append(f"{name}: Invalid negative requirement {required}")
                    continue
                    
                if required > 0 and not constraint.can_accommodate(required):
                    violations.append(f"{name}: need {required}, available {constraint.remaining()}")
            except KeyError:
                violations.append(f"{name}: Constraint not found")
            except Exception as e:
                violations.append(f"{name}: Error checking constraint: {str(e)}")
        
        return len(violations) == 0, violations
    
    def get_order_constraint_value(self, order: Order, constraint_name: str) -> float:
        """Get the constraint value required by this order"""
        constraint_mapping = {
            'quantity': order.quantity,
            'picks': order.picks,
            'hours': order.hours,
            'boxes': order.boxes,
            'changes': 1,  # Each order = 1 changeover
            'austria': 1 if order.country.upper() == 'AUSTRIA' else 0,
            'bvi': 1 if order.brand.upper() == 'BVI' else 0,
        }
        
        return constraint_mapping.get(constraint_name.lower(), 0)
    
    def find_best_line_assignment(self, order: Order) -> Tuple[Optional[str], Dict[str, Any]]:
        """Find the best production line for this order"""
        best_line = None
        best_score = float('inf')
        best_details = {}
        
        for line_name, config in self.line_configs.items():
            # Calculate throughput for this line
            throughput = config.calculate_throughput(order)
            
            # Score this assignment (lower is better)
            # Consider total time and line efficiency
            efficiency_score = throughput['total_time']
            
            if efficiency_score < best_score:
                best_score = efficiency_score
                best_line = line_name
                best_details = {
                    'line': line_name,
                    'total_time': throughput['total_time'],
                    'pick_time': throughput['pick_time'],
                    'wrap_time': throughput['wrap_time'],
                    'seal_time': throughput['seal_time'],
                    'associates': config.total_associates
                }
        
        return best_line, best_details
    
    def schedule_orders(self, target_date: datetime) -> Dict[str, Any]:
        """Main scheduling algorithm"""
        start_time = time.time()
        
        # Reset state
        self.scheduled_orders = []
        self.unscheduled_orders = []
        
        # Reset constraints to initial state
        for constraint in self.constraints.values():
            constraint.current_used = 0.0
        
        # Calculate priorities for all orders
        for order in self.orders:
            order.priority_score = self.calculate_order_priority(order, target_date)
        
        # Sort by priority (highest first) with age preference
        sorted_orders = sorted(self.orders, key=lambda x: (-x.priority_score, x.start_date))
        
        scheduled_count = 0
        constraint_violations = []
        
        # Try to schedule each order
        for order in sorted_orders:
            can_schedule, violations = self.check_constraints(order)
            
            if can_schedule:
                # Find best line assignment
                best_line, line_details = self.find_best_line_assignment(order)
                
                if best_line:
                    # Schedule the order
                    order.assigned_line = best_line
                    order.scheduled_date = target_date
                    
                    # Update constraints
                    for name, constraint in self.constraints.items():
                        required = self.get_order_constraint_value(order, name)
                        constraint.current_used += required
                    
                    self.scheduled_orders.append(order)
                    scheduled_count += 1
                else:
                    # No suitable line found
                    self.unscheduled_orders.append(order)
                    constraint_violations.append(f"Order {order.order_no}: No suitable production line")
            else:
                # Constraint violations
                self.unscheduled_orders.append(order)
                constraint_violations.extend([f"Order {order.order_no}: {v}" for v in violations])
        
        # Calculate summary metrics
        processing_time = time.time() - start_time
        
        total_scheduled_qty = sum(order.quantity for order in self.scheduled_orders)
        total_scheduled_hours = sum(order.hours for order in self.scheduled_orders)
        total_scheduled_picks = sum(order.picks for order in self.scheduled_orders)
        
        # Constraint utilization
        constraint_usage = {}
        for name, constraint in self.constraints.items():
            utilization = (constraint.current_used / constraint.limit * 100) if constraint.limit > 0 else 0
            constraint_usage[name] = {
                'used': constraint.current_used,
                'limit': constraint.limit,
                'remaining': constraint.remaining(),
                'utilization_pct': utilization
            }
        
        return {
            'summary': {
                'total_orders': len(self.orders),
                'scheduled_orders': scheduled_count,
                'unscheduled_orders': len(self.unscheduled_orders),
                'schedule_rate_pct': (scheduled_count / len(self.orders) * 100) if self.orders else 0,
                'total_quantity': sum(order.quantity for order in self.orders),
                'scheduled_quantity': total_scheduled_qty,
                'total_hours': sum(order.hours for order in self.orders),
                'scheduled_hours': total_scheduled_hours,
                'total_picks': sum(order.picks for order in self.orders),
                'scheduled_picks': total_scheduled_picks,
                'processing_time': processing_time
            },
            'constraint_usage': constraint_usage,
            'violations': constraint_violations[:10],  # Limit to first 10 for display
            'line_assignments': self.get_line_assignments()
        }
    
    def get_line_assignments(self) -> Dict[str, Dict[str, Any]]:
        """Get summary of orders assigned to each line"""
        line_summary = {}
        
        for line_name in self.line_configs.keys():
            line_orders = [order for order in self.scheduled_orders if order.assigned_line == line_name]
            
            line_summary[line_name] = {
                'order_count': len(line_orders),
                'total_quantity': sum(order.quantity for order in line_orders),
                'total_hours': sum(order.hours for order in line_orders),
                'total_picks': sum(order.picks for order in line_orders),
                'orders': [order.order_no for order in line_orders]
            }
        
        return line_summary

class SchedulingGUI:
    """User interface for the scheduling engine"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.engine = SchedulingEngine()
        self.results = None
        
        self.setup_gui()
        self.load_default_constraints()
        self.load_default_lines()
    
    def setup_gui(self):
        """Initialize the GUI components"""
        self.root.title(f"Production Scheduling Engine {VERSION}")
        self.root.geometry("900x700")
        
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title_label = ttk.Label(main_frame, text=f"🏭 Production Scheduling Engine {VERSION}", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # File selection section
        file_frame = ttk.LabelFrame(main_frame, text="📁 Data Input", padding="10")
        file_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        
        self.file_path_var = tk.StringVar()
        ttk.Label(file_frame, text="Excel File:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.file_path_var, width=60).grid(row=0, column=1, padx=(5, 5))
        ttk.Button(file_frame, text="Browse", command=self.browse_file).grid(row=0, column=2)
        
        # Target date selection
        ttk.Label(file_frame, text="Target Date:").grid(row=1, column=0, sticky=tk.W, pady=(10, 0))
        self.target_date_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(file_frame, textvariable=self.target_date_var, width=20).grid(row=1, column=1, sticky=tk.W, padx=(5, 5), pady=(10, 0))
        
        # Constraints configuration
        constraints_frame = ttk.LabelFrame(main_frame, text="⚙️ Capacity Constraints", padding="10")
        constraints_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Constraint input fields
        self.constraint_vars = {}
        constraints = [
            ("Quantity", "quantity", 12459),
            ("Picks", "picks", 700),
            ("Hours", "hours", 436),
            ("Changes", "changes", 44),
            ("Austria Orders", "austria", 2),
            ("BVI Orders", "bvi", 50)
        ]
        
        for i, (label, key, default) in enumerate(constraints):
            row = i // 3
            col = (i % 3) * 2
            
            ttk.Label(constraints_frame, text=f"{label}:").grid(row=row, column=col, sticky=tk.W, padx=(0, 5))
            var = tk.StringVar(value=str(default))
            self.constraint_vars[key] = var
            ttk.Entry(constraints_frame, textvariable=var, width=10).grid(row=row, column=col+1, padx=(0, 20))
        
        # Control buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=3, column=0, columnspan=3, pady=20)
        
        ttk.Button(button_frame, text="🔄 Load Data", command=self.load_data).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(button_frame, text="🚀 Create Schedule", command=self.create_schedule).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(button_frame, text="💾 Export Results", command=self.export_results).pack(side=tk.LEFT)
        
        # Results display
        results_frame = ttk.LabelFrame(main_frame, text="📊 Results", padding="10")
        results_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Results text area with scrollbar
        text_frame = ttk.Frame(results_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        self.results_text = tk.Text(text_frame, height=20, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.results_text.yview)
        self.results_text.configure(yscrollcommand=scrollbar.set)
        
        self.results_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready - Load your planning data to begin")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 0))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(4, weight=1)
    
    def load_default_constraints(self):
        """Load default constraint configuration"""
        # These will be updated from GUI inputs
        pass
    
    def load_default_lines(self):
        """Load default production line configurations"""
        # Based on your Line Staffing Options data
        line_configs = [
            LineConfiguration("C1", 4, 1, 2, 1, 55, 55, 20, 15),
            LineConfiguration("C2", 4, 2, 1, 1, 40, 27.5, 40, 15),
            LineConfiguration("C3/4", 5, 2, 2, 1, 27.5, 27.5, 20, 15),
        ]
        
        for config in line_configs:
            self.engine.add_line_configuration(config)
    
    def browse_file(self):
        """Browse for Excel file"""
        file_path = filedialog.askopenfilename(
            title="Select Daily Planning Template",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if file_path:
            self.file_path_var.set(file_path)
    
    def update_constraints(self):
        """Update engine constraints from GUI inputs"""
        self.engine.constraints.clear()
        
        for key, var in self.constraint_vars.items():
            try:
                limit = float(var.get())
                self.engine.add_constraint(key, limit)
            except ValueError:
                messagebox.showerror("Error", f"Invalid constraint value for {key}")
                return False
        
        return True
    
    def load_data(self):
        """Load orders from Excel file"""
        if not self.file_path_var.get():
            messagebox.showerror("Error", "Please select an Excel file first")
            return
            
        def loading_thread():
            try:
                self.status_var.set("Loading data from Excel...")
                self.root.update()
                
                # Load from ReleasedPOOL sheet (based on your template structure)
                df_orders = pd.read_excel(self.file_path_var.get(), sheet_name="ReleasedPOOL")
                
                # Try to load Main sheet for additional order details
                try:
                    df_main = pd.read_excel(self.file_path_var.get(), sheet_name="Main", skiprows=6)
                    # Merge additional details if available
                    if not df_main.empty:
                        df_orders = df_orders.merge(df_main[['Order No', 'Picks', 'Hours', 'Boxes', 'Country', 'Brand']].dropna(), 
                                                  left_on='Order No', right_on='Order No', how='left')
                except FileNotFoundError:
                    print(f"Warning: Main sheet not found in file")
                except pd.errors.EmptyDataError:
                    print(f"Warning: Main sheet is empty")
                except Exception as e:
                    print(f"Warning: Could not load Main sheet details: {str(e)}")
                
                # Helper function to safely convert values with NaN handling
                def safe_int(value, default=0):
                    try:
                        if pd.isna(value):
                            return default
                        return int(float(value))
                    except (ValueError, TypeError):
                        return default
                
                def safe_float(value, default=0.0):
                    try:
                        if pd.isna(value):
                            return default
                        return float(value)
                    except (ValueError, TypeError):
                        return default
                
                def safe_str(value, default=''):
                    try:
                        if pd.isna(value):
                            return default
                        return str(value)
                    except (ValueError, TypeError):
                        return default
                
                # Convert to Order objects
                self.engine.orders = []
                
                for _, row in df_orders.iterrows():
                    try:
                        order = Order(
                            order_no=safe_str(row.get('Order No', '')),
                            part_no=safe_str(row.get('Part No', '')),
                            quantity=safe_int(row.get('Qty', 0)),
                            start_date=pd.to_datetime(row.get('Start Date', datetime.now()), errors='coerce') or datetime.now(),
                            due_date=pd.to_datetime(row.get('Due Date', datetime.now() + timedelta(days=30)), errors='coerce') or (datetime.now() + timedelta(days=30)),
                            picks=safe_int(row.get('Picks', 0)),
                            hours=safe_float(row.get('Hours', 0.0)),
                            boxes=safe_int(row.get('Boxes', 0)),
                            country=safe_str(row.get('Country', '')),
                            brand=safe_str(row.get('Brand', ''))
                        )
                        self.engine.orders.append(order)
                    except Exception as e:
                        print(f"Error processing order {row.get('Order No', 'Unknown')}: {e}")
                        continue
                
                # Update results display
                self.results_text.delete(1.0, tk.END)
                self.results_text.insert(tk.END, f"✅ DATA LOADED SUCCESSFULLY!\n\n")
                self.results_text.insert(tk.END, f"📊 SUMMARY:\n")
                self.results_text.insert(tk.END, f"Orders loaded: {len(self.engine.orders)}\n")
                self.results_text.insert(tk.END, f"Total quantity: {sum(order.quantity for order in self.engine.orders):,}\n")
                self.results_text.insert(tk.END, f"Total hours: {sum(order.hours for order in self.engine.orders):,.1f}\n")
                self.results_text.insert(tk.END, f"Total picks: {sum(order.picks for order in self.engine.orders):,}\n\n")
                
                self.results_text.insert(tk.END, f"🏭 PRODUCTION LINES CONFIGURED:\n")
                for line_name, config in self.engine.line_configs.items():
                    self.results_text.insert(tk.END, f"{line_name}: {config.total_associates} associates\n")
                
                self.status_var.set(f"Data loaded: {len(self.engine.orders)} orders ready for scheduling")
                
                # Update UI in main thread
                self.root.after(0, self.update_load_results)
                
            except Exception as e:
                # Update UI in main thread
                self.root.after(0, lambda: self.handle_load_error(str(e)))
        
        # Disable UI controls during loading
        self.disable_controls()
        threading.Thread(target=loading_thread, daemon=True).start()
    
    def disable_controls(self):
        """Disable UI controls during long operations"""
        for widget in [self.load_button, self.schedule_button, self.export_button]:
            widget.configure(state='disabled')
    
    def enable_controls(self):
        """Re-enable UI controls after long operations"""
        for widget in [self.load_button, self.schedule_button, self.export_button]:
            widget.configure(state='normal')
    
    def handle_load_error(self, error_msg):
        """Handle errors during data loading"""
        messagebox.showerror("Error", f"Failed to load data: {error_msg}")
        self.status_var.set("Error loading data")
        self.enable_controls()
    
    def update_load_results(self):
        """Update UI after successful data load"""
        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(tk.END, f"✅ DATA LOADED SUCCESSFULLY!\n\n")
        self.results_text.insert(tk.END, f"📊 SUMMARY:\n")
        self.results_text.insert(tk.END, f"Orders loaded: {len(self.engine.orders)}\n")
        self.results_text.insert(tk.END, f"Total quantity: {sum(order.quantity for order in self.engine.orders):,}\n")
        self.results_text.insert(tk.END, f"Total hours: {sum(order.hours for order in self.engine.orders):,.1f}\n")
        self.results_text.insert(tk.END, f"Total picks: {sum(order.picks for order in self.engine.orders):,}\n\n")
        
        self.results_text.insert(tk.END, f"🏭 PRODUCTION LINES CONFIGURED:\n")
        for line_name, config in self.engine.line_configs.items():
            self.results_text.insert(tk.END, f"{line_name}: {config.total_associates} associates\n")
        
        self.enable_controls()
    
    def create_schedule(self):
        """Create optimized production schedule"""
        if not self.engine.orders:
            messagebox.showerror("Error", "Please load order data first")
            return
            
        if not self.update_constraints():
            return
            
        def scheduling_thread():
            try:
                self.status_var.set("Creating optimized schedule...")
                self.root.update()
                
                # Parse and validate target date
                target_date = datetime.strptime(self.target_date_var.get(), "%Y-%m-%d")
                if target_date.date() < datetime.now().date():
                    raise ValueError("Target date cannot be in the past")
                
                # Run scheduling engine
                results = self.engine.schedule_orders(target_date)
                self.results = results
                
                # Update UI in main thread
                self.root.after(0, lambda: self.display_results(results))
                
            except ValueError as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Invalid date format. Use YYYY-MM-DD"))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Scheduling failed: {str(e)}"))
            finally:
                self.root.after(0, self.enable_controls)
        
        # Disable UI controls during scheduling
        self.disable_controls()
        threading.Thread(target=scheduling_thread, daemon=True).start()
    
    def display_results(self, results):
        """Display scheduling results"""
        self.results_text.delete(1.0, tk.END)
        
        summary = results['summary']
        
        # Header
        self.results_text.insert(tk.END, f"🚀 PRODUCTION SCHEDULE COMPLETE!\n")
        self.results_text.insert(tk.END, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.results_text.insert(tk.END, f"Target Date: {self.target_date_var.get()}\n\n")
        
        # Summary metrics
        self.results_text.insert(tk.END, f"📊 SCHEDULING SUMMARY:\n")
        self.results_text.insert(tk.END, f"✅ Scheduled Orders: {summary['scheduled_orders']:,} ({summary['schedule_rate_pct']:.1f}%)\n")
        self.results_text.insert(tk.END, f"❌ Unscheduled Orders: {summary['unscheduled_orders']:,}\n")
        self.results_text.insert(tk.END, f"📦 Scheduled Quantity: {summary['scheduled_quantity']:,} / {summary['total_quantity']:,}\n")
        self.results_text.insert(tk.END, f"⏱️ Scheduled Hours: {summary['scheduled_hours']:,.1f} / {summary['total_hours']:,.1f}\n")
        self.results_text.insert(tk.END, f"🎯 Scheduled Picks: {summary['scheduled_picks']:,} / {summary['total_picks']:,}\n")
        self.results_text.insert(tk.END, f"⚡ Processing Time: {summary['processing_time']:.2f} seconds\n\n")
        
        # Constraint utilization
        self.results_text.insert(tk.END, f"📈 CONSTRAINT UTILIZATION:\n")
        for name, usage in results['constraint_usage'].items():
            self.results_text.insert(tk.END, f"{name.title()}: {usage['used']:.0f} / {usage['limit']:.0f} ({usage['utilization_pct']:.1f}% used)\n")
        
        # Line assignments
        self.results_text.insert(tk.END, f"\n🏭 PRODUCTION LINE ASSIGNMENTS:\n")
        for line_name, line_data in results['line_assignments'].items():
            self.results_text.insert(tk.END, f"{line_name}: {line_data['order_count']} orders, {line_data['total_quantity']:,} qty, {line_data['total_hours']:.1f} hrs\n")
        
        # Top constraint violations (if any)
        if results['violations']:
            self.results_text.insert(tk.END, f"\n⚠️ TOP CONSTRAINT VIOLATIONS:\n")
            for violation in results['violations'][:5]:
                self.results_text.insert(tk.END, f"• {violation}\n")
    
    def export_results(self):
        """Export scheduling results to Excel"""
        if not self.results:
            messagebox.showerror("Error", "No results to export. Create a schedule first.")
            return
        
        try:
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = f"Production_Schedule_{VERSION}_{timestamp}.xlsx"
            
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
            scheduled_data = []
            for order in self.engine.scheduled_orders:
                scheduled_data.append({
                    'Order No': order.order_no,
                    'Part No': order.part_no,
                    'Quantity': order.quantity,
                    'Start Date': order.start_date.strftime('%Y-%m-%d'),
                    'Due Date': order.due_date.strftime('%Y-%m-%d'),
                    'Assigned Line': order.assigned_line,
                    'Priority Score': f"{order.priority_score:.2f}",
                    'Picks': order.picks,
                    'Hours': f"{order.hours:.2f}",
                    'Boxes': order.boxes,
                    'Country': order.country,
                    'Brand': order.brand
                })
            
            unscheduled_data = []
            for order in self.engine.unscheduled_orders:
                unscheduled_data.append({
                    'Order No': order.order_no,
                    'Part No': order.part_no,
                    'Quantity': order.quantity,
                    'Start Date': order.start_date.strftime('%Y-%m-%d'),
                    'Due Date': order.due_date.strftime('%Y-%m-%d'),
                    'Priority Score': f"{order.priority_score:.2f}",
                    'Picks': order.picks,
                    'Hours': f"{order.hours:.2f}",
                    'Country': order.country,
                    'Brand': order.brand
                })
            
            # Create summary data
            summary_data = []
            for key, value in self.results['summary'].items():
                summary_data.append({'Metric': key.replace('_', ' ').title(), 'Value': value})
            
            # Constraint usage data
            constraint_data = []
            for name, usage in self.results['constraint_usage'].items():
                constraint_data.append({
                    'Constraint': name.title(),
                    'Limit': usage['limit'],
                    'Used': usage['used'],
                    'Remaining': usage['remaining'],
                    'Utilization %': f"{usage['utilization_pct']:.1f}%"
                })
            
            # Export to Excel
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                pd.DataFrame(scheduled_data).to_excel(writer, sheet_name='Scheduled Orders', index=False)
                pd.DataFrame(unscheduled_data).to_excel(writer, sheet_name='Unscheduled Orders', index=False)
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                pd.DataFrame(constraint_data).to_excel(writer, sheet_name='Constraint Usage', index=False)
            
            self.status_var.set(f"Results exported to {os.path.basename(file_path)}")
            messagebox.showinfo("Success", f"Results exported successfully!\n\nFile: {file_path}")
            
        except Exception as e:
            messagebox.showerror("Error", f"Export failed: {str(e)}")
            self.status_var.set("Export failed")
    
    def run(self):
        """Start the GUI application"""
        # Initial message
        self.results_text.insert(tk.END, f"🏭 Production Scheduling Engine {VERSION}\n")
        self.results_text.insert(tk.END, f"Version: {VERSION} ({VERSION_DATE})\n")
        self.results_text.insert(tk.END, f"Features: {VERSION_NOTES}\n\n")
        
        self.results_text.insert(tk.END, "INSTRUCTIONS:\n")
        self.results_text.insert(tk.END, "1. 📁 Browse and select your Daily Planning Template.xlsm file\n")
        self.results_text.insert(tk.END, "2. ⚙️ Adjust capacity constraints as needed\n")
        self.results_text.insert(tk.END, "3. 🔄 Click 'Load Data' to import orders from ReleasedPOOL sheet\n")
        self.results_text.insert(tk.END, "4. 🚀 Click 'Create Schedule' to optimize production schedule\n")
        self.results_text.insert(tk.END, "5. 💾 Export results to Excel for implementation\n\n")
        
        self.results_text.insert(tk.END, "The engine prioritizes older orders while optimizing for constraints and efficiency.\n")
        self.results_text.insert(tk.END, "Each constraint can be configured independently for maximum flexibility.\n\n")
        
        self.results_text.insert(tk.END, "Ready to begin! Load your planning data to start scheduling. 🎯\n")
        
        self.root.mainloop()

if __name__ == "__main__":
    app = SchedulingGUI()
    app.run()