import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import time
from datetime import datetime

# VERSION INFO
VERSION = "v1.5.0"
VERSION_DATE = "2025-01-23"
DEBUG_MODE = False  # Set to False to disable component debug logging

def process_single_scenario(filepath, scenario_name):
    """Process a single scenario file and return results"""
    
    # Load the sheets we need
    df_main = pd.read_excel(filepath, sheet_name="Main")
    df_stock = pd.read_excel(filepath, sheet_name="StockTally") 
    df_struct = pd.read_excel(filepath, sheet_name="ManStructures")
    df_component_demand = pd.read_excel(filepath, sheet_name="Component Demand")
    df_ipis = pd.read_excel(filepath, sheet_name="IPIS")
    df_hours = pd.read_excel(filepath, sheet_name="Hours")
    df_pos = pd.read_excel(filepath, sheet_name="POs")
    
    # Build stock dictionary (use actual Stock, not Remaining)
    df_stock["PART_NO"] = df_stock["PART_NO"].astype(str)
    stock = df_stock.set_index("PART_NO")["Stock"].to_dict()

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

    # Adjust stock for existing commitments
    for component, committed_qty in committed_components.items():
        if component in stock:
            stock[component] = max(0, stock[component] - committed_qty)
        else:
            stock[component] = 0
        
    # Build labor standards dictionary
    df_hours["PART_NO"] = df_hours["PART_NO"].astype(str)
    labor_standards = df_hours.groupby("PART_NO")["Hours per Unit"].sum().to_dict()
    
    # Build BOM structure
    struct = df_struct[df_struct["Component Part"].notna()].copy()
    struct["Parent Part"] = struct["Parent Part"].astype(str)
    struct["Component Part"] = struct["Component Part"].astype(str)
    
    # Sort by Start Date for proper sequential allocation
    df_main['Start Date'] = pd.to_datetime(df_main['Start Date'], errors='coerce')
    df_main = df_main.sort_values(['Start Date', 'SO Number'], na_position='last').reset_index(drop=True)
    
    results = []
    processed = 0
    total = len(df_main)

    # Process each order sequentially
    for _, row in df_main.iterrows():
        processed += 1
        
        so = str(row["SO Number"]) if pd.notna(row["SO Number"]) else f"ORDER_{processed}"
        part = str(row["Part"]) if pd.notna(row["Part"]) else None
        demand_qty = row["Demand"] if pd.notna(row["Demand"]) and row["Demand"] > 0 else 0
        planner = str(row["Planner"]) if pd.notna(row["Planner"]) else "UNKNOWN"
        start_date = row["Start Date"]
        
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
                "Components": "Missing part number or zero demand",
                "Shortages": "-"
            })
            continue
        
        # Check if this is a piggyback order
        try:
            pb_check = f"NS{part}99"
            is_pb = "PB" if pb_check in struct["Component Part"].values else "-"
        except:
            is_pb = "-"
        
        # Get BOM for this part
        try:
            bom = struct[struct["Parent Part"] == part]
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
            # This part has components - use ALL-OR-NOTHING allocation
            all_components_available = True
            component_requirements = []
            
            for _, comp in bom.iterrows():
                try:
                    comp_part = str(comp["Component Part"])
                    qpa = comp["QpA"] if pd.notna(comp["QpA"]) else 1
                    required_qty = int(qpa * demand_qty)
                    available = stock.get(comp_part, 0)
                    
                    component_requirements.append({
                        'part': comp_part,
                        'required': required_qty,
                        'available': available
                    })
                    
                    components_needed[comp_part] = required_qty
                    
                    # Check availability but DON'T allocate yet
                    if available < required_qty:
                        all_components_available = False
                        shortage = required_qty - available
                        
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
                            shortage_details.append(f"{comp_part} (need {required_qty}, have {available}, short {shortage})")
                                
                except Exception as e:
                    all_components_available = False
                    shortage_details.append(f"Component processing error: {str(e)}")
                    continue
            
            # If ALL components available, THEN allocate all of them
            if all_components_available:
                releasable = True
                for req in component_requirements:
                    comp_part = req['part']
                    required_qty = req['required']
                    stock[comp_part] -= required_qty
            else:
                releasable = False

        else:
            # This is a raw material/purchased part
            try:
                available = stock.get(part, 0)
                if available >= demand_qty:
                    stock[part] -= demand_qty
                    releasable = True
                else:
                    releasable = False
                    shortage = demand_qty - available
                    shortage_details.append(f"{part} (need {demand_qty}, have {available}, short {shortage})")
            except:
                releasable = False
                shortage_details.append(f"{part} (stock lookup failed)")

        # Build result record
        shortage_parts_only = []
        components_info = "; ".join(shortage_details) if shortage_details else str(components_needed) if components_needed else "-"

        # Extract just the part numbers from shortage details
        for detail in shortage_details:
            # Handle different shortage detail formats
            if " short " in detail and "–" in detail:
                # Format: "8034441 short 185 – PO P123456 due 2025-06-15"
                part_short = detail.split(" short ")[0].strip()
                shortage_parts_only.append(part_short)
            elif "(" in detail and " (need " in detail:
                # Format: "952809BID (need 250, have 0, short 250)"
                part_short = detail.split(" (need ")[0].strip()
                shortage_parts_only.append(part_short)
            elif "(" in detail:
                # Fallback: anything before first parenthesis
                part_short = detail.split("(")[0].strip()
                shortage_parts_only.append(part_short)
            else:
                # If no recognizable pattern, use first word
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
            "Components": components_info,
            "Shortages": clean_shortages
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
    
    return {
        'name': scenario_name,
        'filepath': filepath,
        'results_df': df_results,
        'metrics': {
            'total_orders': total_orders,
            'releasable_count': releasable_count,
            'held_count': held_count,
            'pb_count': pb_count,
            'skipped_count': skipped_count,
            'total_hours': total_hours,
            'releasable_hours': releasable_hours,
            'held_hours': held_hours,
            'committed_parts_count': committed_parts_count,
            'total_committed_qty': total_committed_qty
        }
    }

def load_and_process_files():
    # Multiple file selection
    filepaths = filedialog.askopenfilenames(
        title="Select Material Demand Files (Hold Ctrl for multiple scenarios)",
        filetypes=[("Excel files", "*.xlsm *.xlsx")]
    )
    if not filepaths:
        return

    try:
        start_time = time.time()
        main_frame.configure(style='TFrame')
        
        scenarios = []
        
        # Progress callback function
        def update_progress(message):
            status_var.set(message)
            root.update()
        
        # Process each file as a scenario
        for i, filepath in enumerate(filepaths):
            filename = os.path.basename(filepath)
            scenario_name = f"Scenario_{i+1}_{filename.replace('.xlsm', '').replace('.xlsx', '')}"
            
            # Calculate estimate for this scenario if we have data from previous ones
            if i > 0:
                total_elapsed = time.time() - start_time
                avg_time_per_scenario = total_elapsed / i
                estimated_this_scenario = avg_time_per_scenario
                update_progress(f"📊 Processing Scenario {i+1}/{len(filepaths)}: {filename} (Est. ~{estimated_this_scenario:.0f}s)")
            else:
                update_progress(f"📊 Processing Scenario {i+1}/{len(filepaths)}: {filename}")
            
            # Simple processing - no complex callbacks
            scenario_start_time = time.time()
            scenario_result = process_single_scenario(filepath, scenario_name)
            scenario_end_time = time.time()
            scenario_duration = scenario_end_time - scenario_start_time
            
            scenarios.append(scenario_result)
            
            # Show completion with actual metrics and time
            metrics = scenario_result['metrics']
            
            # Calculate remaining time estimate for display
            if i < len(filepaths) - 1:
                total_elapsed = time.time() - start_time
                avg_time_per_scenario = total_elapsed / (i + 1)
                remaining_scenarios = len(filepaths) - (i + 1)
                estimated_remaining = remaining_scenarios * avg_time_per_scenario
                
                update_progress(f"✅ Scenario {i+1}/{len(filepaths)} complete: {metrics['releasable_count']:,}/{metrics['total_orders']:,} releasable ({scenario_duration:.1f}s) | {estimated_remaining:.0f}s remaining")
            else:
                update_progress(f"✅ Scenario {i+1}/{len(filepaths)} complete: {metrics['releasable_count']:,}/{metrics['total_orders']:,} releasable ({scenario_duration:.1f}s)")
            
            # Brief pause to show completion
            time.sleep(1.5)
        
        # Calculate total processing time
        end_time = time.time()
        processing_time = end_time - start_time
        total_orders_processed = sum(s['metrics']['total_orders'] for s in scenarios)
        orders_per_second = total_orders_processed / processing_time if processing_time > 0 else 0
        
        # Save results
        output_dir = os.path.dirname(filepaths[0])
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        
        if len(scenarios) > 1:
            output_file = os.path.join(output_dir, f"Multi_Scenario_Analysis_{VERSION}_{timestamp}.xlsx")
        else:
            output_file = os.path.join(output_dir, f"Material_Release_Plan_{VERSION}_{timestamp}.xlsx")
        
        status_var.set("💾 Saving multi-scenario results...")
        root.update()
        
        # Create Excel with multiple scenarios
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Write each scenario to its own sheet
            for scenario in scenarios:
                sheet_name = scenario['name'][:31]  # Excel sheet name limit
                scenario['results_df'].to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Create scenario comparison summary
            if len(scenarios) > 1:
                comparison_data = []
                for scenario in scenarios:
                    metrics = scenario['metrics']
                    comparison_data.append({
                        'Scenario': scenario['name'],
                        'File': os.path.basename(scenario['filepath']),
                        'Total Orders': metrics['total_orders'],
                        'Releasable Orders': metrics['releasable_count'],
                        'Held Orders': metrics['held_count'],
                        'Release Rate (%)': f"{metrics['releasable_count']/metrics['total_orders']*100:.1f}%" if metrics['total_orders'] > 0 else "0%",
                        'Piggyback Orders': metrics['pb_count'],
                        'Total Hours': f"{metrics['total_hours']:,.1f}",
                        'Releasable Hours': f"{metrics['releasable_hours']:,.1f}",
                        'Labor Release Rate (%)': f"{metrics['releasable_hours']/metrics['total_hours']*100:.1f}%" if metrics['total_hours'] > 0 else "0%",
                        'Committed Parts': metrics['committed_parts_count'],
                        'Committed Qty': f"{metrics['total_committed_qty']:,}"
                    })
                
                comparison_df = pd.DataFrame(comparison_data)
                comparison_df.to_excel(writer, sheet_name='Scenario Comparison', index=False)
            
            # Create summary sheet
            summary_data = pd.DataFrame({
                'Metric': [
                    'Tool Version',
                    'Processing Date',
                    'Number of Scenarios',
                    'Total Orders Processed',
                    'Total Processing Time (seconds)',
                    'Orders per Second',
                    'Average Time per Scenario (seconds)',
                    'Files Processed'
                ],
                'Value': [
                    f"{VERSION} ({VERSION_DATE})",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    len(scenarios),
                    f"{total_orders_processed:,}",
                    f"{processing_time:.2f}",
                    f"{orders_per_second:.1f}",
                    f"{processing_time/len(scenarios):.2f}",
                    "; ".join([os.path.basename(s['filepath']) for s in scenarios])
                ]
            })
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
        
        # Display results
        if len(scenarios) > 1:
            # Multi-scenario summary - COMPARE instead of add
            best_scenario = max(scenarios, key=lambda s: s['metrics']['releasable_count'])
            worst_scenario = min(scenarios, key=lambda s: s['metrics']['releasable_count'])
            improvement = best_scenario['metrics']['releasable_count'] - worst_scenario['metrics']['releasable_count']
            typical_order_count = scenarios[0]['metrics']['total_orders']  # Define it here
            
            summary_text = f"""✅ MULTI-SCENARIO ANALYSIS COMPLETE!

📊 SCENARIOS COMPARED: {len(scenarios)}
📈 ORDERS PER SCENARIO: ~{typical_order_count:,}

🏆 BEST PERFORMER: {os.path.basename(best_scenario['filepath'])}
   ✅ {best_scenario['metrics']['releasable_count']:,} releasable orders ({best_scenario['metrics']['releasable_count']/best_scenario['metrics']['total_orders']*100:.1f}%)

📉 BASELINE: {os.path.basename(worst_scenario['filepath'])}
   ✅ {worst_scenario['metrics']['releasable_count']:,} releasable orders ({worst_scenario['metrics']['releasable_count']/worst_scenario['metrics']['total_orders']*100:.1f}%)

🔺 IMPROVEMENT: +{improvement:,} more orders releasable ({improvement/worst_scenario['metrics']['total_orders']*100:.1f}% boost)

"""
            for i, scenario in enumerate(scenarios):
                metrics = scenario['metrics']
                summary_text += f"""🔸 SCENARIO {i+1}: {os.path.basename(scenario['filepath'])}
   Orders: {metrics['total_orders']:,} (✅{metrics['releasable_count']:,} ❌{metrics['held_count']:,})
   Release Rate: {metrics['releasable_count']/metrics['total_orders']*100:.1f}%
   Hours: {metrics['releasable_hours']:,.0f}/{metrics['total_hours']:,.0f} ({metrics['releasable_hours']/metrics['total_hours']*100:.1f}%)

"""
            
            summary_text += f"""⏱️ PERFORMANCE METRICS:
   Total Processing Time: {processing_time:.2f} seconds
   Processing Speed: {orders_per_second:.1f} orders/second
   Average per Scenario: {processing_time/len(scenarios):.1f} seconds
   
💾 Results saved to:
   {os.path.basename(output_file)}
   
📊 COMPARISON FEATURES:
   ✓ Each scenario in separate sheet
   ✓ Side-by-side comparison table
   ✓ Performance metrics for all scenarios"""
        else:
            # Single scenario summary (same as before)
            scenario = scenarios[0]
            metrics = scenario['metrics']
            summary_text = f"""✅ PROCESSING COMPLETE!

📊 RESULTS SUMMARY:
   Total Orders: {metrics['total_orders']:,}
   ✅ Releasable: {metrics['releasable_count']:,} ({metrics['releasable_count']/metrics['total_orders']*100:.1f}%)
   ❌ On Hold: {metrics['held_count']:,} ({metrics['held_count']/metrics['total_orders']*100:.1f}%)
   🏷️ Piggyback: {metrics['pb_count']:,}
   ⚠️ Skipped: {metrics['skipped_count']:,}

⏱️ LABOR HOURS SUMMARY:
   Total Hours: {metrics['total_hours']:,.1f}
   ✅ Releasable Hours: {metrics['releasable_hours']:,.1f} ({metrics['releasable_hours']/metrics['total_hours']*100:.1f}%)
   ❌ Held Hours: {metrics['held_hours']:,.1f} ({metrics['held_hours']/metrics['total_hours']*100:.1f}%)

📦 COMPONENT COMMITMENTS:
   Parts with Existing Orders: {metrics['committed_parts_count']:,}
   Total Committed Quantity: {metrics['total_committed_qty']:,}

⏱️ PERFORMANCE METRICS:
   Processing Time: {processing_time:.2f} seconds
   Orders per Second: {orders_per_second:.1f}

💾 Results saved to:
   {os.path.basename(output_file)}"""
        
        results_text.delete(1.0, tk.END)
        results_text.insert(1.0, summary_text)
        
        # For status bar, show comparison instead of totals
        if len(scenarios) > 1:
            best_scenario = max(scenarios, key=lambda s: s['metrics']['releasable_count'])
            worst_scenario = min(scenarios, key=lambda s: s['metrics']['releasable_count'])
            improvement = best_scenario['metrics']['releasable_count'] - worst_scenario['metrics']['releasable_count']
            
            status_var.set(f"✅ Complete! {len(scenarios)} scenarios compared - Best: {best_scenario['metrics']['releasable_count']:,} releasable (+{improvement:,} vs worst) in {processing_time:.1f}s")
        else:
            total_orders = scenarios[0]['metrics']['total_orders']
            total_releasable = scenarios[0]['metrics']['releasable_count']
            status_var.set(f"✅ Complete! {total_releasable:,}/{total_orders:,} orders releasable in {processing_time:.1f}s")
            
        main_frame.configure(style='Success.TFrame')
        
    except Exception as e:
        messagebox.showerror("Error", f"Processing failed:\n\n{str(e)}")
        status_var.set("❌ Processing failed")

def copy_summary_to_clipboard():
    summary_text_content = results_text.get("1.0", tk.END).strip()
    root.clipboard_clear()
    root.clipboard_append(summary_text_content)
    root.update()
    copy_btn.config(text="✅ Copied!")
    root.after(2000, lambda: copy_btn.config(text="📋 Copy Summary"))

# Create GUI
root = tk.Tk()
root.title(f"Multi-Scenario Material Planner {VERSION}")
root.geometry("750x750")

# Main frame
main_frame = ttk.Frame(root, padding="20")
main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

# Title
title_label = ttk.Label(main_frame, text=f"Multi-Scenario Material Planning Tool {VERSION}", 
                       font=('Arial', 16, 'bold'))
title_label.grid(row=0, column=0, pady=(0, 20))

# Instructions
instructions = """This tool processes Material Check Data Files and determines which shop orders can be released.

NEW MULTI-SCENARIO FEATURES:
• Select MULTIPLE files to compare different scenarios (hold Ctrl)
• Each scenario gets its own sheet in the output
• Automatic side-by-side comparison table
• Perfect for "what-if" analysis and planning alternatives
• Real-time progress tracking with time estimates

CORE FEATURES:
• Sorts orders by start date (proper sequential allocation)
• All-or-nothing material allocation (no partial consumption)
• Identifies piggyback (PB) orders automatically
• Provides detailed shortage analysis with PO information
• Exports comprehensive results with summary metrics
• Shows processing speed and estimated completion time

Click the button below to select your Excel file(s) and start processing."""

inst_label = ttk.Label(main_frame, text=instructions, justify=tk.LEFT, wraplength=700)
inst_label.grid(row=1, column=0, pady=(0, 20))

# Process button
process_btn = ttk.Button(main_frame, text="📂 SELECT FILES & PROCESS", 
                        command=load_and_process_files, 
                        style='Big.TButton')
process_btn.grid(row=2, column=0, pady=(0, 20))

# Configure button style
style = ttk.Style()
style.configure('Big.TButton', font=('Arial', 12, 'bold'))
style.configure('Success.TFrame', background='#7ff09a')

# Status
status_var = tk.StringVar()
status_var.set("🔄 Ready - Select Excel file(s) to begin (Ctrl+click for multiple scenarios) | Now with progress tracking!")
status_label = ttk.Label(main_frame, textvariable=status_var, font=('Arial', 10))
status_label.grid(row=3, column=0, pady=(0, 10), sticky=tk.W)

# Results area
results_frame = ttk.LabelFrame(main_frame, text="📊 Results", padding="10")
results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))

results_text = tk.Text(results_frame, height=18, width=90, font=('Consolas', 9))
scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=results_text.yview)
results_text.configure(yscrollcommand=scrollbar.set)

results_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

copy_btn = ttk.Button(main_frame, text="📋 Copy Summary", command=copy_summary_to_clipboard)
copy_btn.grid(row=5, column=0, pady=(10, 10))

# Configure grid weights
root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)
main_frame.columnconfigure(0, weight=1)
main_frame.rowconfigure(4, weight=1)
results_frame.columnconfigure(0, weight=1)
results_frame.rowconfigure(0, weight=1)

# Show initial message
results_text.insert(1.0, "Select your Excel file(s) to begin material release planning...\n\nNEW FEATURES:\n• Hold Ctrl to select multiple files for scenario comparison!\n• Real-time progress tracking with time estimates\n• Enhanced shortage analysis and reporting")

if __name__ == "__main__":
    root.mainloop()