import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import time
from datetime import datetime

# VERSION INFO
VERSION = "v1.3.0"
VERSION_DATE = "2025-01-23"
VERSION_NOTES = "Added labor hours, IPIS/POs support, component commitments"

def load_and_process_file():
    # File selection
    filepath = filedialog.askopenfilename(
        title="Select Material Check Dictionary",
        filetypes=[("Excel files", "*.xlsm *.xlsx")]
    )
    if not filepath:
        return

    try:
        # Start timing
        start_time = time.time()
        
        # Update status
        status_var.set("📂 Loading Excel sheets...")
        root.update()
        
        # Load the sheets we need
        df_main = pd.read_excel(filepath, sheet_name="Main")
        df_stock = pd.read_excel(filepath, sheet_name="StockTally") 
        df_struct = pd.read_excel(filepath, sheet_name="ManStructures")
        df_component_demand = pd.read_excel(filepath, sheet_name="Component Demand")
        df_ipis = pd.read_excel(filepath, sheet_name="IPIS")
        df_hours = pd.read_excel(filepath, sheet_name="Hours")
        df_pos = pd.read_excel(filepath, sheet_name="POs")  # Keep for future use
        
        status_var.set("⚙️ Processing existing component commitments...")
        root.update()
        
        # Build stock dictionary (use actual Stock, not Remaining)
        df_stock["PART_NO"] = df_stock["PART_NO"].astype(str)
        stock = df_stock.set_index("PART_NO")["Stock"].to_dict()
        
        # Build labor standards dictionary
        df_hours["PART_NO"] = df_hours["PART_NO"].astype(str)
        labor_standards = df_hours.groupby("PART_NO")["Hours per Unit"].sum().to_dict()
        
        # CRITICAL: Account for existing component commitments
        # Subtract already committed component quantities from available stock
        committed_components = {}
        committed_parts_count = 0
        total_committed_qty = 0
        
        if not df_component_demand.empty:
            # Group component demand by part to get total committed quantities
            df_component_demand["Component Part Number"] = df_component_demand["Component Part Number"].astype(str)
            committed_summary = df_component_demand.groupby("Component Part Number")["Component Qty Required"].sum()
            committed_components = committed_summary.to_dict()
            
            # Calculate summary stats
            committed_parts_count = len(committed_components)
            total_committed_qty = sum(committed_components.values())
            
            # Subtract committed quantities from available stock
            for component, committed_qty in committed_components.items():
                if component in stock:
                    stock[component] = max(0, stock[component] - committed_qty)  # Don't go negative
                else:
                    stock[component] = 0  # If not in stock list, treat as zero available
        
        status_var.set("⚙️ Processing BOM structures...")
        root.update()
        
        # Build BOM structure
        struct = df_struct[df_struct["Component Part"].notna()].copy()
        # Convert part numbers to strings to handle mixed types
        struct["Parent Part"] = struct["Parent Part"].astype(str)
        struct["Component Part"] = struct["Component Part"].astype(str)
        
        # CRITICAL: Sort by Start Date for proper sequential allocation
        df_main['Start Date'] = pd.to_datetime(df_main['Start Date'], errors='coerce')
        # Handle missing dates by putting them at the end
        df_main = df_main.sort_values(['Start Date', 'SO Number'], na_position='last').reset_index(drop=True)
        
        results = []
        processed = 0
        total = len(df_main)
        
        # Process each order sequentially
        for _, row in df_main.iterrows():
            processed += 1
            progress_pct = processed / total * 100
            status_var.set(f"⚙️ Processing order {processed}/{total} ({progress_pct:.1f}%)...")
            root.update()
            
            so = str(row["SO Number"]) if pd.notna(row["SO Number"]) else f"ORDER_{processed}"
            part = str(row["Part"]) if pd.notna(row["Part"]) else None
            demand_qty = row["Demand"] if pd.notna(row["Demand"]) and row["Demand"] > 0 else 0
            planner = str(row["Planner"]) if pd.notna(row["Planner"]) else "UNKNOWN"
            start_date = row["Start Date"]
            
            # Skip orders with missing critical data
            if part is None or part == "nan" or demand_qty <= 0:
                # Add a record showing why this order was skipped
                results.append({
                    "SO Number": so,
                    "Part": part or "MISSING",
                    "Planner": planner,
                    "Start Date": start_date.strftime('%Y-%m-%d') if pd.notna(start_date) else "No Date",
                    "PB": "-",
                    "Demand": demand_qty,
                    "Hours": 0,
                    "Status": "⚠️ Skipped",
                    "Material Shortages": "Missing part number or zero demand",
                    "Components": "-"
                })
                continue
            
            # Check if this is a piggyback order (safely)
            try:
                pb_check = f"NS{part}99"
                is_pb = "PB" if pb_check in struct["Component Part"].values else "-"
            except:
                is_pb = "-"
            
            # Get BOM for this part (safely)
            try:
                bom = struct[struct["Parent Part"] == part]
            except:
                bom = pd.DataFrame()  # Empty dataframe if lookup fails
            
            # Check material availability
            releasable = True
            shortage_details = []
            components_needed = {}
            
            # Calculate labor hours for this order
            base_hours = labor_standards.get(part, 0)
            labor_hours = base_hours * demand_qty
            
            if len(bom) > 0:
                # This part has components
                for _, comp in bom.iterrows():
                    try:
                        comp_part = str(comp["Component Part"])
                        qpa = comp["QpA"] if pd.notna(comp["QpA"]) else 1
                        required_qty = int(qpa * demand_qty)
                        available = stock.get(comp_part, 0)
                        
                        components_needed[comp_part] = required_qty
                        
                        if available >= required_qty:
                            # Allocate the material (consume from stock)
                            stock[comp_part] -= required_qty
                        else:
                            releasable = False
                            shortage = required_qty - available
                            shortage_details.append(f"{comp_part} (need {required_qty}, have {available}, short {shortage})")
                    except Exception as e:
                        # Skip this component if there's an error
                        releasable = False
                        shortage_details.append(f"Component processing error: {str(e)}")
                        continue
            else:
                # This is a raw material/purchased part
                try:
                    available = stock.get(part, 0)
                    if available >= demand_qty:
                        stock[part] -= demand_qty
                    else:
                        releasable = False
                        shortage = demand_qty - available
                        shortage_details.append(f"{part} (need {demand_qty}, have {available}, short {shortage})")
                except:
                    # If stock lookup fails, treat as shortage
                    releasable = False
                    shortage_details.append(f"{part} (stock lookup failed)")
            
            # Build result record
            results.append({
                "SO Number": so,
                "Part": part,
                "Planner": planner,
                "Start Date": start_date.strftime('%Y-%m-%d') if pd.notna(start_date) else "No Date",
                "PB": is_pb,
                "Demand": demand_qty,
                "Hours": round(labor_hours, 4),
                "Status": "✅ Release" if releasable else "❌ Hold",
                "Material Shortages": "; ".join(shortage_details) if shortage_details else "-",
                "Components": str(components_needed) if components_needed else "-"
            })
        
        # Generate summary
        df_results = pd.DataFrame(results)
        
        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time
        
        total_orders = len(df_results)
        releasable_count = len(df_results[df_results['Status'] == '✅ Release'])
        held_count = total_orders - releasable_count
        pb_count = len(df_results[df_results['PB'] == 'PB'])
        skipped_count = len(df_results[df_results['Status'] == '⚠️ Skipped'])
        
        # Calculate labor hours
        total_hours = df_results['Hours'].sum()
        releasable_hours = df_results[df_results['Status'] == '✅ Release']['Hours'].sum()
        held_hours = df_results[df_results['Status'] == '❌ Hold']['Hours'].sum()
        
        # Calculate processing rate
        orders_per_second = total_orders / processing_time if processing_time > 0 else 0
        
        # Save results
        output_dir = os.path.dirname(filepath)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        output_file = os.path.join(output_dir, f"Material_Release_Plan_{VERSION}_{timestamp}.xlsx")
        
        status_var.set("💾 Saving results...")
        root.update()
        
        # Save with summary
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_results.to_excel(writer, sheet_name='Release Plan', index=False)
            
            # Summary sheet
            summary_data = pd.DataFrame({
                'Metric': [
                    'Tool Version',
                    'Processing Date',
                    'Total Orders',
                    'Releasable Orders', 
                    'Held Orders',
                    'Skipped Orders',
                    'Release Rate (%)',
                    'Piggyback Orders',
                    'Total Labor Hours',
                    'Releasable Labor Hours',
                    'Held Labor Hours',
                    'Labor Release Rate (%)',
                    'Parts with Existing Commitments',
                    'Total Committed Component Qty',
                    'Processing Time (seconds)',
                    'Orders per Second',
                    'Avg Time per Order (ms)'
                ],
                'Value': [
                    f"{VERSION} ({VERSION_DATE})",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    total_orders,
                    releasable_count,
                    held_count,
                    skipped_count,
                    f"{releasable_count/total_orders*100:.1f}%",
                    pb_count,
                    f"{total_hours:,.1f}",
                    f"{releasable_hours:,.1f}",
                    f"{held_hours:,.1f}",
                    f"{releasable_hours/total_hours*100:.1f}%" if total_hours > 0 else "0%",
                    committed_parts_count,
                    total_committed_qty,
                    f"{processing_time:.2f}",
                    f"{orders_per_second:.1f}",
                    f"{processing_time/total_orders*1000:.1f}"
                ]
            })
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
        
        # Show results
        summary_text = f"""✅ PROCESSING COMPLETE! (Tool {VERSION})

📊 RESULTS SUMMARY:
   Total Orders: {total_orders:,}
   ✅ Releasable: {releasable_count:,} ({releasable_count/total_orders*100:.1f}%)
   ❌ On Hold: {held_count:,} ({held_count/total_orders*100:.1f}%)
   🏷️  Piggyback: {pb_count:,}
   ⚠️  Skipped: {skipped_count:,}

⏱️ LABOR HOURS SUMMARY:
   Total Hours: {total_hours:,.1f}
   ✅ Releasable Hours: {releasable_hours:,.1f} ({releasable_hours/total_hours*100:.1f}%)
   ❌ Held Hours: {held_hours:,.1f} ({held_hours/total_hours*100:.1f}%)

📦 COMPONENT COMMITMENTS:
   Parts with Existing Orders: {committed_parts_count:,}
   Total Committed Quantity: {total_committed_qty:,}

⏱️ PERFORMANCE METRICS:
   Processing Time: {processing_time:.2f} seconds
   Orders/Second: {orders_per_second:.1f}
   Average Time/Order: {processing_time/total_orders*1000:.1f} ms

💾 Results saved to:
   {os.path.basename(output_file)}

🎯 KEY FEATURES ({VERSION}):
   ✓ Sorted by start date (proper sequential allocation)
   ✓ Accounts for existing component commitments
   ✓ Material consumption tracking  
   ✓ Piggyback order detection
   ✓ Labor hour calculations
   ✓ Detailed shortage analysis
   ✓ Excel output with summary sheet"""
        
        results_text.delete(1.0, tk.END)
        results_text.insert(1.0, summary_text)
        
        status_var.set(f"✅ Complete! {releasable_count}/{total_orders} orders releasable in {processing_time:.1f}s")
        
        messagebox.showinfo("Processing Complete!", 
                          f"Material release plan complete! ({VERSION})\n\n"
                          f"📊 ORDERS:\n"
                          f"✅ {releasable_count} releasable orders\n"
                          f"❌ {held_count} held orders\n"
                          f"⚠️ {skipped_count} skipped orders\n\n"
                          f"⏱️ LABOR HOURS:\n"
                          f"✅ {releasable_hours:,.1f} releasable hours\n"
                          f"❌ {held_hours:,.1f} held hours\n"
                          f"📊 {releasable_hours/total_hours*100:.1f}% labor releasable\n\n"
                          f"📦 Accounted for {committed_parts_count} parts with existing commitments\n"
                          f"📊 Total committed qty: {total_committed_qty:,}\n\n"
                          f"⏱️ Processed in {processing_time:.2f} seconds\n"
                          f"📊 Rate: {orders_per_second:.1f} orders/second\n\n"
                          f"Results saved to:\n{os.path.basename(output_file)}")
        
    except Exception as e:
        messagebox.showerror("Error", f"Processing failed:\n\n{str(e)}")
        status_var.set("❌ Processing failed")

        # Create GUI
root = tk.Tk()
root.title(f"🏭 Material Release Planner {VERSION}")
root.geometry("700x500")

# Main frame
main_frame = ttk.Frame(root, padding="20")
main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

# Title
title_label = ttk.Label(main_frame, text=f"🏭 Material Release Planning Tool {VERSION}", 
                       font=('Arial', 16, 'bold'))
title_label.grid(row=0, column=0, pady=(0, 20))

# Instructions
instructions = """This tool processes your Material Check Dictionary and determines which shop orders can be released.

KEY FEATURES:
✓ Sorts orders by start date (proper sequential material allocation)  
✓ Consumes materials as each order is processed
✓ Identifies piggyback (PB) orders automatically
✓ Provides detailed shortage analysis
✓ Exports results to Excel with summary

Click the button below to select your Excel file and start processing."""

inst_label = ttk.Label(main_frame, text=instructions, justify=tk.LEFT, wraplength=600)
inst_label.grid(row=1, column=0, pady=(0, 20))

# Process button
process_btn = ttk.Button(main_frame, text="📂 SELECT FILE & PROCESS", 
                        command=load_and_process_file, 
                        style='Big.TButton')
process_btn.grid(row=2, column=0, pady=(0, 20))

# Configure button style
style = ttk.Style()
style.configure('Big.TButton', font=('Arial', 12, 'bold'))

# Status
status_var = tk.StringVar()
status_var.set("🔄 Ready - Click button to select Excel file")
status_label = ttk.Label(main_frame, textvariable=status_var, font=('Arial', 10))
status_label.grid(row=3, column=0, pady=(0, 10), sticky=tk.W)

# Results area
results_frame = ttk.LabelFrame(main_frame, text="📊 Results", padding="10")
results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))

results_text = tk.Text(results_frame, height=15, width=80, font=('Consolas', 9))
scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=results_text.yview)
results_text.configure(yscrollcommand=scrollbar.set)

results_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

# Configure grid weights
root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)
main_frame.columnconfigure(0, weight=1)
main_frame.rowconfigure(4, weight=1)
results_frame.columnconfigure(0, weight=1)
results_frame.rowconfigure(0, weight=1)

# Show initial message
results_text.insert(1.0, "Select your Excel file to begin material release planning...")

if __name__ == "__main__":
    root.mainloop()