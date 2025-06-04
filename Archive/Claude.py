import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import threading
import os

@dataclass
class ShopOrder:
    so_number: str
    part_number: str
    planner: str
    start_date: datetime
    demand_qty: int
    is_piggyback: bool = False
    status: str = "Planned"
    labor_hours: float = 0.0
    components_needed: Dict[str, int] = None
    blocking_materials: List[str] = None
    
    def __post_init__(self):
        if self.components_needed is None:
            self.components_needed = {}
        if self.blocking_materials is None:
            self.blocking_materials = []

class MaterialPlanningEngine:
    def __init__(self):
        # Core data structures
        self.shop_orders: List[ShopOrder] = []
        self.stock_levels: Dict[str, int] = {}
        self.bom_structure: Dict[str, Dict[str, int]] = {}
        self.labor_standards: Dict[str, float] = {}
        self.purchase_orders: Dict[str, Dict] = {}
        
        # Processing state
        self.available_stock: Dict[str, int] = {}
        self.allocation_log: List[Dict] = []
        
    def load_data_from_excel(self, file_path: str, progress_callback=None):
        """Load data from Excel file with progress updates"""
        try:
            if progress_callback:
                progress_callback("📂 Opening Excel file...")
            
            # Read all sheets
            excel_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
            
            if progress_callback:
                progress_callback("📋 Loading shop orders...")
            
            # Load shop orders from Demand sheet
            if 'Demand' in excel_data:
                demand_df = excel_data['Demand']
                for _, row in demand_df.iterrows():
                    try:
                        so = ShopOrder(
                            so_number=str(row['SO No']),
                            part_number=str(row['Part No']),
                            planner=str(row['Planner']),
                            start_date=pd.to_datetime(row['Start Date']),
                            demand_qty=int(row['Rev Qty Due']),
                            status=row['Status'] if 'Status' in row else 'Planned'
                        )
                        self.shop_orders.append(so)
                    except Exception as e:
                        continue
            
            if progress_callback:
                progress_callback("📦 Loading stock levels...")
            
            # Load stock levels
            if 'StockTally' in excel_data:
                stock_df = excel_data['StockTally']
                for _, row in stock_df.iterrows():
                    try:
                        part_no = str(row['PART_NO'])
                        stock_qty = int(row['Stock']) if pd.notna(row['Stock']) else 0
                        self.stock_levels[part_no] = stock_qty
                    except Exception as e:
                        continue
            
            if progress_callback:
                progress_callback("🔧 Loading BOM structures...")
            
            # Load BOM structure
            if 'ManStructures' in excel_data:
                bom_df = excel_data['ManStructures']
                for _, row in bom_df.iterrows():
                    try:
                        parent = str(row['Parent Part'])
                        component = str(row['Component Part'])
                        qty_per = int(row['QpA']) if pd.notna(row['QpA']) else 1
                        
                        if parent not in self.bom_structure:
                            self.bom_structure[parent] = {}
                        self.bom_structure[parent][component] = qty_per
                    except Exception as e:
                        continue
            
            if progress_callback:
                progress_callback("⏱️ Loading labor standards...")
            
            # Load labor standards
            if 'Hours' in excel_data:
                hours_df = excel_data['Hours']
                labor_by_part = hours_df.groupby('PART_NO')['Hours per Unit'].sum()
                self.labor_standards = labor_by_part.to_dict()
            
            if progress_callback:
                progress_callback("🛒 Loading purchase orders...")
            
            # Load PO data
            if 'POs' in excel_data:
                po_df = excel_data['POs']
                for _, row in po_df.iterrows():
                    try:
                        po_num = str(row['PO Number'])
                        self.purchase_orders[po_num] = {
                            'part_number': str(row['Part Number']),
                            'qty_due': int(row['Qty Due']),
                            'due_date': pd.to_datetime(row['Promised Due Date']),
                            'supplier': str(row['Supplier']) if 'Supplier' in row else ''
                        }
                    except Exception as e:
                        continue
            
            return True
            
        except Exception as e:
            raise Exception(f"Error loading Excel file: {str(e)}")
    
    def identify_piggyback_orders(self):
        """Identify piggyback orders based on BOM structure"""
        for order in self.shop_orders:
            pb_part = f"NS{order.part_number}99"
            if any(pb_part in components for components in self.bom_structure.values()):
                order.is_piggyback = True
    
    def explode_bom_requirements(self, order: ShopOrder) -> Dict[str, int]:
        """Calculate all component requirements for a shop order"""
        requirements = defaultdict(int)
        
        def explode_recursive(part_number: str, quantity: int, level: int = 0):
            if part_number in self.bom_structure:
                for component, qty_per in self.bom_structure[part_number].items():
                    total_needed = quantity * qty_per
                    requirements[component] += total_needed
                    explode_recursive(component, total_needed, level + 1)
            else:
                requirements[part_number] += quantity
        
        explode_recursive(order.part_number, order.demand_qty)
        return dict(requirements)
    
    def check_material_availability(self, order: ShopOrder) -> Tuple[bool, List[str]]:
        """Check if all materials are available for an order"""
        components_needed = self.explode_bom_requirements(order)
        order.components_needed = components_needed
        
        shortages = []
        can_release = True
        
        for component, qty_needed in components_needed.items():
            available = self.available_stock.get(component, 0)
            if available < qty_needed:
                shortages.append(f"{component}: need {qty_needed}, have {available}")
                can_release = False
        
        order.blocking_materials = shortages
        return can_release, shortages
    
    def allocate_materials(self, order: ShopOrder):
        """Allocate materials to an order and update running stock levels"""
        for component, qty_needed in order.components_needed.items():
            if component in self.available_stock:
                self.available_stock[component] -= qty_needed
                self.allocation_log.append({
                    'so_number': order.so_number,
                    'part_allocated': component,
                    'qty_allocated': qty_needed,
                    'remaining_stock': self.available_stock[component],
                    'timestamp': datetime.now()
                })
    
    def calculate_labor_hours(self, order: ShopOrder):
        """Calculate total labor hours for an order"""
        base_hours = self.labor_standards.get(order.part_number, 0)
        order.labor_hours = base_hours * order.demand_qty
    
    def find_relevant_pos(self, shortages: List[str]) -> List[str]:
        """Find PO numbers for parts that are short"""
        relevant_pos = []
        for shortage in shortages:
            part = shortage.split(':')[0]
            for po_num, po_data in self.purchase_orders.items():
                if po_data['part_number'] == part:
                    relevant_pos.append(po_num)
        return list(set(relevant_pos))
    
    def get_due_info(self, comments: str) -> str:
        """Extract due date information from PO comments"""
        if comments == "Release" or comments == "REVIEW SHORTAGES":
            return "-"
        
        due_info = []
        pos = comments.split("; ")
        for po in pos:
            if po in self.purchase_orders:
                po_data = self.purchase_orders[po]
                due_date = po_data['due_date'].strftime('%d/%m/%Y')
                due_info.append(f"{po}: Due: {due_date}")
        
        return "; ".join(due_info) if due_info else "REVIEW SHORTAGES"
    
    def process_release_plan(self, progress_callback=None) -> pd.DataFrame:
        """Main processing engine"""
        if progress_callback:
            progress_callback("🚀 Starting material planning...")
        
        # Initialize available stock
        self.available_stock = self.stock_levels.copy()
        
        # Sort orders by start date (CRITICAL for sequential processing)
        self.shop_orders.sort(key=lambda x: (x.start_date, x.so_number))
        
        if progress_callback:
            progress_callback("🏷️ Identifying piggyback orders...")
        self.identify_piggyback_orders()
        
        results = []
        total_orders = len(self.shop_orders)
        
        for i, order in enumerate(self.shop_orders):
            if progress_callback and i % 100 == 0:
                progress_callback(f"⚙️ Processing order {i+1}/{total_orders}")
            
            # Check material availability
            can_release, shortages = self.check_material_availability(order)
            
            # Calculate labor hours
            self.calculate_labor_hours(order)
            
            # Determine status and comments
            if can_release:
                order.status = "Release"
                self.allocate_materials(order)
                current_comments = "Release"
                po_comments = "Release"
            else:
                order.status = "Hold - Material"
                blocking_pos = self.find_relevant_pos(shortages)
                current_comments = "; ".join(blocking_pos) if blocking_pos else "REVIEW SHORTAGES"
                po_comments = current_comments
            
            # Build result record
            result = {
                'SO Number': order.so_number,
                'Part': order.part_number,
                'Planner': order.planner,
                'Start Date': order.start_date,
                'PB': "PB" if order.is_piggyback else "-",
                'Demand': order.demand_qty,
                'Hours': round(order.labor_hours, 4),
                'Current Comments': current_comments,
                'PO Comments': po_comments,
                'Due Info': self.get_due_info(current_comments),
                'Status': order.status
            }
            
            results.append(result)
        
        if progress_callback:
            progress_callback("✅ Processing complete!")
        
        return pd.DataFrame(results)

class MaterialPlanningGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Material Release Planning System")
        self.root.geometry("900x650")
        self.root.configure(bg='#f0f0f0')
        
        # Configure style
        style = ttk.Style()
        style.theme_use('clam')
        
        self.engine = MaterialPlanningEngine()
        self.results_df = None
        
        self.create_widgets()
        
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title_label = ttk.Label(main_frame, text="🏭 Material Release Planning System", 
                               font=('Arial', 18, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # File selection section
        file_frame = ttk.LabelFrame(main_frame, text="📁 Step 1: Select Your Excel File", padding="15")
        file_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 15))
        
        self.file_path_var = tk.StringVar()
        file_entry = ttk.Entry(file_frame, textvariable=self.file_path_var, width=60, font=('Arial', 10))
        file_entry.grid(row=0, column=0, padx=(0, 10))
        
        browse_btn = ttk.Button(file_frame, text="📂 Browse Files", command=self.browse_file)
        browse_btn.grid(row=0, column=1)
        
        # Processing section
        process_frame = ttk.LabelFrame(main_frame, text="⚙️ Step 2: Process Your Data", padding="15")
        process_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 15))
        
        # Big process button
        self.process_btn = ttk.Button(process_frame, text="🚀 PROCESS FILE", 
                                     command=self.process_file, style='Big.TButton')
        self.process_btn.grid(row=0, column=0, padx=(0, 20))
        
        # Configure big button style
        style = ttk.Style()
        style.configure('Big.TButton', font=('Arial', 12, 'bold'))
        
        # Export button (initially disabled)
        self.export_btn = ttk.Button(process_frame, text="💾 Export Results", 
                                    command=self.export_results, state='disabled')
        self.export_btn.grid(row=0, column=1)
        
        # Progress section
        progress_frame = ttk.LabelFrame(main_frame, text="📊 Progress", padding="10")
        progress_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 15))
        
        self.progress_var = tk.StringVar()
        self.progress_var.set("🔄 Ready - Select an Excel file to begin")
        progress_label = ttk.Label(progress_frame, textvariable=self.progress_var, font=('Arial', 10))
        progress_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate', length=400)
        self.progress_bar.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 5))
        
        # Results section
        results_frame = ttk.LabelFrame(main_frame, text="📈 Results Summary", padding="10")
        results_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Results display with scrollbar
        text_frame = ttk.Frame(results_frame)
        text_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.results_text = tk.Text(text_frame, height=18, width=85, font=('Consolas', 10))
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.results_text.yview)
        self.results_text.configure(yscrollcommand=scrollbar.set)
        
        self.results_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready to process material data")
        status_label = ttk.Label(main_frame, textvariable=self.status_var, 
                                font=('Arial', 9), foreground='blue')
        status_label.grid(row=5, column=0, columnspan=3, sticky=tk.W, pady=(10, 0))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(2, weight=1)
        main_frame.rowconfigure(4, weight=1)
        file_frame.columnconfigure(0, weight=1)
        progress_frame.columnconfigure(0, weight=1)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        text_frame.columnconfigure(0, weight=1)
        text_frame.rowconfigure(0, weight=1)
        
        # Add initial instructions
        self.show_instructions()
        
    def show_instructions(self):
        instructions = """🏭 MATERIAL RELEASE PLANNING SYSTEM

This tool analyzes your shop orders and determines which ones can be released based on material availability.

INSTRUCTIONS:
1. Click 'Browse Files' and select your 'Automated Material Check Dictionary.xlsm' file
2. Click 'PROCESS FILE' to run the analysis
3. Review the results summary below
4. Click 'Export Results' to save the output

WHAT IT DOES:
✓ Loads all your shop orders, stock levels, BOMs, and PO data
✓ Processes orders sequentially by start date (critical for accurate allocation)
✓ Identifies piggyback (PB) orders automatically
✓ Calculates material requirements and availability
✓ Shows which orders can be released vs. held for materials
✓ Provides detailed shortage analysis and PO information

Ready to process your data!"""

        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(1.0, instructions)
    
    def browse_file(self):
        filename = filedialog.askopenfilename(
            title="Select Material Check Dictionary Excel File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if filename:
            self.file_path_var.set(filename)
            self.status_var.set(f"✅ File selected: {os.path.basename(filename)} - Ready to process!")
            self.process_btn.configure(state='normal')
    
    def update_progress(self, message):
        """Update progress message"""
        self.progress_var.set(message)
        self.root.update()
    
    def process_file(self):
        if not self.file_path_var.get():
            messagebox.showerror("Error", "Please select an Excel file first!")
            return
        
        def processing_thread():
            try:
                # Disable the process button
                self.process_btn.configure(state='disabled')
                self.progress_bar.start()
                
                # Reset engine
                self.engine = MaterialPlanningEngine()
                
                # Load data
                self.update_progress("📂 Loading data from Excel file...")
                self.engine.load_data_from_excel(self.file_path_var.get(), self.update_progress)
                
                # Process the release plan
                self.update_progress("⚙️ Running material planning analysis...")
                self.results_df = self.engine.process_release_plan(self.update_progress)
                
                # Generate summary
                self.generate_summary()
                
                # Enable export
                self.export_btn.configure(state='normal')
                self.status_var.set("✅ Processing complete! Results ready for export.")
                
            except Exception as e:
                messagebox.showerror("Processing Error", f"Failed to process file:\n\n{str(e)}")
                self.status_var.set("❌ Processing failed - check your Excel file")
            finally:
                self.progress_bar.stop()
                self.progress_var.set("🔄 Ready for next file")
                self.process_btn.configure(state='normal')
        
        threading.Thread(target=processing_thread, daemon=True).start()
    
    def generate_summary(self):
        """Generate and display results summary"""
        if self.results_df is None:
            return
        
        total_orders = len(self.results_df)
        releasable = len(self.results_df[self.results_df['Current Comments'] == 'Release'])
        held = total_orders - releasable
        
        total_hours = self.results_df['Hours'].sum()
        releasable_hours = self.results_df[self.results_df['Current Comments'] == 'Release']['Hours'].sum()
        
        # Count piggyback orders
        pb_orders = len(self.results_df[self.results_df['PB'] == 'PB'])
        
        # Top shortage analysis
        shortage_counts = defaultdict(int)
        for _, row in self.results_df.iterrows():
            if row['Current Comments'] not in ['Release', 'REVIEW SHORTAGES']:
                pos = row['Current Comments'].split('; ')
                for po in pos:
                    if po in self.engine.purchase_orders:
                        part = self.engine.purchase_orders[po]['part_number']
                        shortage_counts[part] += 1
        
        # Planner breakdown
        planner_summary = self.results_df.groupby('Planner').agg({
            'SO Number': 'count',
            'Hours': 'sum'
        }).round(1)
        
        summary = f"""🎯 MATERIAL RELEASE PLANNING - RESULTS SUMMARY
{'='*60}

📊 OVERALL STATISTICS:
   Total Shop Orders:     {total_orders:,}
   ✅ Releasable:         {releasable:,} ({releasable/total_orders*100:.1f}%)
   ⏸️  On Hold:            {held:,} ({held/total_orders*100:.1f}%)
   🏷️  Piggyback Orders:   {pb_orders:,}

⏱️ LABOR HOURS SUMMARY:
   Total Hours:           {total_hours:,.1f}
   Releasable Hours:      {releasable_hours:,.1f} ({releasable_hours/total_hours*100:.1f}%)
   Held Hours:            {total_hours - releasable_hours:,.1f}

🚨 TOP MATERIAL SHORTAGES:"""

        for part, count in sorted(shortage_counts.items(), key=lambda x: x[1], reverse=True)[:8]:
            summary += f"\n   {part}: affects {count} orders"

        summary += f"""

👥 BY PLANNER:"""
        for planner, data in planner_summary.head(10).iterrows():
            summary += f"\n   {planner}: {int(data['SO Number'])} orders, {data['Hours']:.1f} hours"

        summary += f"""

💡 RECOMMENDATIONS:
   • Focus on resolving top shortage parts to unlock the most orders
   • Prioritize orders with earlier start dates
   • Review piggyback orders for two-stage planning
   • Export results for detailed analysis in Excel

✅ Analysis complete! Click 'Export Results' to save detailed data."""

        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(1.0, summary)
    
    def export_results(self):
        if self.results_df is None:
            messagebox.showerror("Error", "No results to export. Process a file first.")
            return
        
        # Auto-suggest filename and default to Desktop
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        suggested_name = f"MaterialPlan_Results_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        
        filename = filedialog.asksaveasfilename(
            title="Save Material Planning Results",
            initialfile=suggested_name,
            initialdir=desktop,
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("CSV files", "*.csv")]
        )
        
        if filename:
            try:
                # Show progress
                self.status_var.set("💾 Exporting results...")
                self.root.update()
                
                if filename.endswith('.xlsx'):
                    try:
                        # Try the fancy multi-sheet approach first
                        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                            self.results_df.to_excel(writer, sheet_name='Release Plan', index=False)
                            
                            # Add summary sheet
                            summary_data = {
                                'Metric': ['Total Orders', 'Releasable Orders', 'Held Orders', 'Total Hours', 'Releasable Hours'],
                                'Value': [
                                    len(self.results_df),
                                    len(self.results_df[self.results_df['Current Comments'] == 'Release']),
                                    len(self.results_df[self.results_df['Current Comments'] != 'Release']),
                                    self.results_df['Hours'].sum(),
                                    self.results_df[self.results_df['Current Comments'] == 'Release']['Hours'].sum()
                                ]
                            }
                            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                    except Exception as excel_error:
                        # If multi-sheet fails, try simple Excel export
                        print(f"Multi-sheet failed, trying simple: {excel_error}")
                        self.results_df.to_excel(filename, index=False, engine='openpyxl')
                        
                elif filename.endswith('.csv'):
                    self.results_df.to_csv(filename, index=False)
                else:
                    # Default to CSV if extension is unclear
                    filename = filename + '.csv'
                    self.results_df.to_csv(filename, index=False)
                
                # Verify file was created
                if os.path.exists(filename):
                    file_size = os.path.getsize(filename)
                    messagebox.showinfo("✅ Export Successful!", 
                                      f"Results exported successfully!\n\n"
                                      f"📁 File: {os.path.basename(filename)}\n"
                                      f"📍 Location: {os.path.dirname(filename)}\n"
                                      f"📊 Size: {file_size:,} bytes\n"
                                      f"📝 Records: {len(self.results_df):,}")
                    self.status_var.set(f"✅ Successfully exported {len(self.results_df):,} records to {os.path.basename(filename)}")
                else:
                    raise Exception("File was not created successfully")
                
            except PermissionError:
                messagebox.showerror("❌ Permission Error", 
                                   "Cannot write to that location!\n\n"
                                   "Try saving to:\n"
                                   "• Your Desktop\n"
                                   "• Your Documents folder\n"
                                   "• Make sure the file isn't already open")
                self.status_var.set("❌ Export failed - permission denied")
                
            except Exception as e:
                # Fallback: try saving to Desktop as CSV
                try:
                    fallback_file = os.path.join(desktop, f"MaterialPlan_Backup_{datetime.now().strftime('%H%M%S')}.csv")
                    self.results_df.to_csv(fallback_file, index=False)
                    messagebox.showwarning("⚠️ Partial Success", 
                                         f"Original export failed, but saved backup CSV to Desktop:\n\n"
                                         f"📁 {os.path.basename(fallback_file)}\n\n"
                                         f"Original error: {str(e)}")
                    self.status_var.set(f"⚠️ Backup saved to Desktop as CSV")
                except:
                    messagebox.showerror("❌ Export Failed", 
                                       f"Could not export results:\n\n{str(e)}\n\n"
                                       f"Try:\n"
                                       f"• Saving to a different location\n"
                                       f"• Choosing CSV format instead\n"
                                       f"• Running as administrator")
                    self.status_var.set("❌ Export failed - see error message")
    
    def run(self):
        self.root.mainloop()

def main():
    app = MaterialPlanningGUI()
    app.run()

if __name__ == "__main__":
    main()