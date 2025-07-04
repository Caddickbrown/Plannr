import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import time
from datetime import datetime

# VERSION INFO
VERSION = "v1.8.0"
VERSION_DATE = "2025-01-25"
DEBUG_MODE = False

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

def process_single_scenario(filepath, scenario_name, status_callback=None, scenario_num=1, total_scenarios=1, sorting_strategy=None):
    """Process a single scenario file and return results with live progress updates"""
    
    if status_callback:
        strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
        status_callback(f"📂 [Scenario {scenario_num}/{total_scenarios}] Loading sheets for {os.path.basename(filepath)} ({strategy_name})...")
    
    # Load the sheets we need
    df_main = pd.read_excel(filepath, sheet_name="Main")
    df_stock = pd.read_excel(filepath, sheet_name="StockTally") 
    df_struct = pd.read_excel(filepath, sheet_name="ManStructures")
    df_component_demand = pd.read_excel(filepath, sheet_name="Component Demand")
    df_ipis = pd.read_excel(filepath, sheet_name="IPIS")
    df_hours = pd.read_excel(filepath, sheet_name="Hours")
    df_pos = pd.read_excel(filepath, sheet_name="POs")
    
    if status_callback:
        strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
        status_callback(f"⚙️ [Scenario {scenario_num}/{total_scenarios}] Processing commitments ({strategy_name})...")
    
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
    
    if status_callback:
        strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
        status_callback(f"⚙️ [Scenario {scenario_num}/{total_scenarios}] Building BOM structures ({strategy_name})...")
    
    # Build BOM structure
    struct = df_struct[df_struct["Component Part"].notna()].copy()
    struct["Parent Part"] = struct["Parent Part"].astype(str)
    struct["Component Part"] = struct["Component Part"].astype(str)
    
    # Pre-process main data
    df_main['Start Date'] = pd.to_datetime(df_main['Start Date'], errors='coerce')
    df_main["Part"] = df_main["Part"].astype(str)
    df_main["Planner"] = df_main["Planner"].fillna("UNKNOWN").astype(str)
    df_main["Demand"] = pd.to_numeric(df_main["Demand"], errors='coerce').fillna(0)
    
    # Calculate hours for sorting
    df_main["Hours_Calc"] = df_main.apply(lambda row: 
        labor_standards.get(str(row["Part"]), 0) * row["Demand"], axis=1)
    
    # Apply sorting strategy
    if sorting_strategy:
        # Handle missing values appropriately for each column type
        for col in sorting_strategy["columns"]:
            if col == "Start Date":
                # Put NaT (missing dates) at the end
                df_main = df_main.sort_values(sorting_strategy["columns"], 
                                            ascending=sorting_strategy["ascending"], 
                                            na_position='last')
            else:
                df_main = df_main.sort_values(sorting_strategy["columns"], 
                                            ascending=sorting_strategy["ascending"])
    else:
        # Default sorting (original behavior)
        df_main = df_main.sort_values(['Start Date', 'SO Number'], na_position='last')
    
    df_main = df_main.reset_index(drop=True)
    
    results = []
    processed = 0
    total = len(df_main)
    
    # Baseline estimate: ~0.15 seconds per order (conservative estimate)
    baseline_time_per_order = 0.15
    processing_start_time = time.time()

    # Process each order sequentially with FREQUENT UI updates + TIME ESTIMATES
    for _, row in df_main.iterrows():
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
            
            if status_callback:
                strategy_name = sorting_strategy["name"] if sorting_strategy else "Default"
                if processed == total:
                    status_callback(f"✅ [Scenario {scenario_num}/{total_scenarios}] {os.path.basename(filepath)} ({strategy_name}) - Completed {total:,} orders in {elapsed:.1f}s")
                else:
                    # Show current scenario progress + context about remaining scenarios
                    remaining_scenarios = total_scenarios - scenario_num
                    if remaining_scenarios > 0:
                        status_callback(f"⚙️ [Scenario {scenario_num}/{total_scenarios}] {strategy_name} - {processed:,}/{total:,} ({progress_pct:.1f}%) | {est_remaining:.0f}s + {remaining_scenarios} more")
                    else:
                        status_callback(f"⚙️ [Scenario {scenario_num}/{total_scenarios}] {strategy_name} - {processed:,}/{total:,} ({progress_pct:.1f}%) | {est_remaining:.0f}s remaining")
        
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
                "Shortages": "-",
                "Components": "Missing part number or zero demand"
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
    
    return {
        'name': scenario_name,
        'filepath': filepath,
        'sorting_strategy': sorting_strategy["name"] if sorting_strategy else "Default (Start Date)",
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
            'total_qty': total_qty,
            'releasable_qty': releasable_qty,
            'held_qty': held_qty,
            'committed_parts_count': committed_parts_count,
            'total_committed_qty': total_committed_qty
        }
    }

def load_and_process_files():
    # Multiple file selection
    filepaths = filedialog.askopenfilenames(
        title="Select Material Demand Files (Hold Ctrl for multiple files)",
        filetypes=[("Excel files", "*.xlsm *.xlsx")]
    )
    if not filepaths:
        # Reset to ready state if cancelled
        status_var.set("🔄 Ready - Select Excel file(s) to begin processing")
        return

    try:
        # Clear the UI and start fresh
        status_var.set("🔄 Initializing...")
        root.update_idletasks()
        
        start_time = time.time()
        main_frame.configure(style='TFrame')
        
        scenarios = []
        scenarios_for_comparison = []  # Will store all tested scenarios for comparison tables
        
        # Progress callback function that updates UI immediately
        def update_progress(message):
            status_var.set(message)
            root.update_idletasks()
        
        # Determine processing mode
        minmax_mode = minmax_var.get()
        
        if minmax_mode:
            # Min/Max optimization mode - test all sorting strategies
            strategies = get_sorting_strategies()
            total_scenarios = len(filepaths) * len(strategies)
            
            update_progress(f"🔥 MIN/MAX MODE: Testing {len(strategies)} sorting strategies on {len(filepaths)} file(s) = {total_scenarios} total scenarios")
            time.sleep(1)
            
            scenario_num = 0
            all_strategy_results = []  # Store ALL results for comparison
            
            for file_idx, filepath in enumerate(filepaths):
                filename = os.path.basename(filepath)
                base_filename = filename.replace('.xlsm', '').replace('.xlsx', '')
                file_strategy_results = []  # Results for this specific file
                
                for strategy_idx, strategy in enumerate(strategies):
                    scenario_num += 1
                    scenario_name = f"{base_filename}_{strategy['name'].replace(' ', '_').replace('(', '').replace(')', '')}"
                    
                    # Process with specific sorting strategy
                    scenario_start_time = time.time()
                    scenario_result = process_single_scenario(
                        filepath, scenario_name, update_progress, 
                        scenario_num, total_scenarios, strategy
                    )
                    scenario_end_time = time.time()
                    scenario_duration = scenario_end_time - scenario_start_time
                    
                    # Store result for this file
                    file_strategy_results.append(scenario_result)
                    all_strategy_results.append(scenario_result)
                    
                    # Show completion
                    metrics = scenario_result['metrics']
                    remaining_scenarios = total_scenarios - scenario_num
                    
                    if remaining_scenarios > 0:
                        total_elapsed = time.time() - start_time
                        avg_time_per_scenario = total_elapsed / scenario_num
                        estimated_remaining = remaining_scenarios * avg_time_per_scenario
                        
                        update_progress(f"✅ [{scenario_num}/{total_scenarios}] {strategy['name']}: {metrics['releasable_count']:,}/{metrics['total_orders']:,} orders ({scenario_duration:.1f}s) | {estimated_remaining:.0f}s remaining")
                    else:
                        update_progress(f"✅ [{scenario_num}/{total_scenarios}] {strategy['name']}: {metrics['releasable_count']:,}/{metrics['total_orders']:,} orders ({scenario_duration:.1f}s) | OPTIMIZATION COMPLETE!")
                    
                    time.sleep(0.2)  # Brief pause between strategies
                
                # After testing all strategies for this file, find the best ones
                best_orders_strategy = max(file_strategy_results, key=lambda s: s['metrics']['releasable_count'])
                best_hours_strategy = max(file_strategy_results, key=lambda s: s['metrics']['releasable_hours'])
                best_qty_strategy = max(file_strategy_results, key=lambda s: s['metrics']['releasable_qty'])
                
                # Create NEW scenario objects with clear names for the best strategies
                # Best Orders Strategy
                best_orders_scenario = {
                    'name': f"BEST_ORDERS_{base_filename}",
                    'filepath': filepath,
                    'sorting_strategy': f"🏆 BEST ORDERS: {best_orders_strategy['sorting_strategy']}",
                    'results_df': best_orders_strategy['results_df'],
                    'metrics': best_orders_strategy['metrics']
                }
                scenarios.append(best_orders_scenario)
                
                # Best Hours Strategy
                best_hours_scenario = {
                    'name': f"BEST_HOURS_{base_filename}",
                    'filepath': filepath,
                    'sorting_strategy': f"🏆 BEST HOURS: {best_hours_strategy['sorting_strategy']}",
                    'results_df': best_hours_strategy['results_df'],
                    'metrics': best_hours_strategy['metrics']
                }
                scenarios.append(best_hours_scenario)
                
                # Best Quantity Strategy
                best_qty_scenario = {
                    'name': f"BEST_QTY_{base_filename}",
                    'filepath': filepath,
                    'sorting_strategy': f"🏆 BEST QTY: {best_qty_strategy['sorting_strategy']}",
                    'results_df': best_qty_strategy['results_df'],
                    'metrics': best_qty_strategy['metrics']
                }
                scenarios.append(best_qty_scenario)
                
                update_progress(f"🏆 File {file_idx+1}/{len(filepaths)} optimized: Orders={best_orders_strategy['sorting_strategy']} ({best_orders_strategy['metrics']['releasable_count']:,}), Hours={best_hours_strategy['sorting_strategy']} ({best_hours_strategy['metrics']['releasable_hours']:,.0f}), Qty={best_qty_strategy['sorting_strategy']} ({best_qty_strategy['metrics']['releasable_qty']:,})")
                time.sleep(0.5)
            
            # Use all_strategy_results for comparison tables
            scenarios_for_comparison = all_strategy_results
        else:
            # Standard mode - process files normally
            total_scenarios = len(filepaths)
            
            for i, filepath in enumerate(filepaths):
                filename = os.path.basename(filepath)
                scenario_name = f"Scenario_{i+1}_{filename.replace('.xlsm', '').replace('.xlsx', '')}"
                scenario_num = i + 1
                
                # Calculate estimate for this scenario
                if i > 0:
                    total_elapsed = time.time() - start_time
                    avg_time_per_scenario = total_elapsed / i
                    remaining_scenarios = len(filepaths) - i
                    total_est_remaining = remaining_scenarios * avg_time_per_scenario
                    update_progress(f"📊 [Scenario {scenario_num}/{len(filepaths)}] Starting: {filename} | Est. {total_est_remaining:.0f}s for all remaining scenarios")
                else:
                    update_progress(f"📊 [Scenario {scenario_num}/{len(filepaths)}] Starting: {filename}")
                
                # Process with live progress updates
                scenario_start_time = time.time()
                scenario_result = process_single_scenario(filepath, scenario_name, update_progress, scenario_num, len(filepaths))
                scenario_end_time = time.time()
                scenario_duration = scenario_end_time - scenario_start_time
                
                scenarios.append(scenario_result)
                scenarios_for_comparison.append(scenario_result)  # Same as scenarios in standard mode
                
                # Show completion with actual metrics and time
                metrics = scenario_result['metrics']
                
                if i < len(filepaths) - 1:
                    total_elapsed = time.time() - start_time
                    avg_time_per_scenario = total_elapsed / (i + 1)
                    remaining_scenarios = len(filepaths) - (i + 1)
                    estimated_remaining = remaining_scenarios * avg_time_per_scenario
                    
                    update_progress(f"✅ [Scenario {scenario_num}/{len(filepaths)}] Complete: {metrics['releasable_count']:,}/{metrics['total_orders']:,} releasable ({scenario_duration:.1f}s) | Est. {estimated_remaining:.0f}s for {remaining_scenarios} remaining scenarios")
                else:
                    update_progress(f"✅ [Scenario {scenario_num}/{len(filepaths)}] Complete: {metrics['releasable_count']:,}/{metrics['total_orders']:,} releasable ({scenario_duration:.1f}s) | COMPLETE!")
                
                time.sleep(0.3)
        
        # Calculate total processing time
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Calculate total orders processed (unique orders, not duplicated across strategies)
        if minmax_mode:
            # In min/max mode, count unique orders processed (not duplicated across strategies)
            unique_files = set([s['filepath'] for s in scenarios])
            total_orders_processed = sum([s['metrics']['total_orders'] for s in scenarios if s['filepath'] in unique_files])
            # Remove duplicates by taking only first occurrence of each file
            seen_files = set()
            unique_scenarios = []
            for s in scenarios:
                if s['filepath'] not in seen_files:
                    unique_scenarios.append(s)
                    seen_files.add(s['filepath'])
            total_orders_processed = sum([s['metrics']['total_orders'] for s in unique_scenarios])
        else:
            total_orders_processed = sum(s['metrics']['total_orders'] for s in scenarios)
            
        orders_per_second = total_orders_processed / processing_time if processing_time > 0 else 0
        
        # Save results
        output_dir = os.path.dirname(filepaths[0])
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        
        if minmax_mode:
            output_file = os.path.join(output_dir, f"MinMax_Optimization_Analysis_{VERSION}_{timestamp}.xlsx")
        elif len(scenarios) > 1:
            output_file = os.path.join(output_dir, f"Multi_Scenario_Analysis_{VERSION}_{timestamp}.xlsx")
        else:
            output_file = os.path.join(output_dir, f"Material_Release_Plan_{VERSION}_{timestamp}.xlsx")
        
        status_var.set("💾 Saving optimization results...")
        root.update_idletasks()
        
        # Create Excel with multiple scenarios
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Write each scenario to its own sheet
            for scenario in scenarios:
                sheet_name = scenario['name'][:31]  # Excel sheet name limit
                scenario['results_df'].to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Create scenario comparison summary
            if len(scenarios_for_comparison) > 1:
                comparison_data = []
                for scenario in scenarios_for_comparison:
                    metrics = scenario['metrics']
                    comparison_data.append({
                        'Scenario': scenario['name'],
                        'File': os.path.basename(scenario['filepath']),
                        'Sorting Strategy': scenario['sorting_strategy'],
                        'Total Orders': metrics['total_orders'],
                        'Releasable Orders': metrics['releasable_count'],
                        'Held Orders': metrics['held_count'],
                        'Release Rate (%)': f"{metrics['releasable_count']/metrics['total_orders']*100:.1f}%" if metrics['total_orders'] > 0 else "0%",
                        'Total Qty': f"{metrics['total_qty']:,}",
                        'Releasable Qty': f"{metrics['releasable_qty']:,}",
                        'Qty Release Rate (%)': f"{metrics['releasable_qty']/metrics['total_qty']*100:.1f}%" if metrics['total_qty'] > 0 else "0%",
                        'Piggyback Orders': metrics['pb_count'],
                        'Total Hours': f"{metrics['total_hours']:,.1f}",
                        'Releasable Hours': f"{metrics['releasable_hours']:,.1f}",
                        'Labor Release Rate (%)': f"{metrics['releasable_hours']/metrics['total_hours']*100:.1f}%" if metrics['total_hours'] > 0 else "0%",
                        'Committed Parts': metrics['committed_parts_count'],
                        'Committed Qty': f"{metrics['total_committed_qty']:,}"
                    })
                
                comparison_df = pd.DataFrame(comparison_data)
                comparison_df.to_excel(writer, sheet_name='Strategy Comparison', index=False)
                
                # If min/max mode, create optimization summary
                if minmax_mode:
                    # Group by file and find best strategies
                    opt_summary = []
                    files_processed = list(set([s['filepath'] for s in scenarios_for_comparison]))
                    
                    for filepath in files_processed:
                        file_scenarios = [s for s in scenarios_for_comparison if s['filepath'] == filepath]
                        
                        # Find best by orders, hours, and quantity
                        best_orders = max(file_scenarios, key=lambda s: s['metrics']['releasable_count'])
                        best_hours = max(file_scenarios, key=lambda s: s['metrics']['releasable_hours'])
                        best_qty = max(file_scenarios, key=lambda s: s['metrics']['releasable_qty'])
                        worst_orders = min(file_scenarios, key=lambda s: s['metrics']['releasable_count'])
                        
                        opt_summary.append({
                            'File': os.path.basename(filepath),
                            'Best Strategy (Orders)': best_orders['sorting_strategy'],
                            'Best Orders Released': best_orders['metrics']['releasable_count'],
                            'Best Strategy (Hours)': best_hours['sorting_strategy'],
                            'Best Hours Released': f"{best_hours['metrics']['releasable_hours']:,.1f}",
                            'Best Strategy (Qty)': best_qty['sorting_strategy'],
                            'Best Qty Released': f"{best_qty['metrics']['releasable_qty']:,}",
                            'Worst Orders Released': worst_orders['metrics']['releasable_count'],
                            'Order Improvement': best_orders['metrics']['releasable_count'] - worst_orders['metrics']['releasable_count'],
                            'Order Improvement (%)': f"{((best_orders['metrics']['releasable_count'] - worst_orders['metrics']['releasable_count']) / worst_orders['metrics']['total_orders'] * 100):.1f}%"
                        })
                    
                    opt_df = pd.DataFrame(opt_summary)
                    opt_df.to_excel(writer, sheet_name='Optimization Summary', index=False)
            
            # Create summary sheet
            summary_data = pd.DataFrame({
                'Metric': [
                    'Tool Version',
                    'Processing Date',
                    'Processing Mode',
                    'Files Processed',
                    'Strategies Tested' if minmax_mode else 'Number of Scenarios',
                    'Best Strategies Saved' if minmax_mode else 'Total Scenarios',
                    'Total Orders Processed',
                    'Total Processing Time (seconds)',
                    'Orders per Second',
                    'Average Time per Strategy' if minmax_mode else 'Average Time per Scenario (seconds)',
                    'Files Processed'
                ],
                'Value': [
                    f"{VERSION} ({VERSION_DATE})",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Min/Max Optimization" if minmax_mode else "Standard",
                    len(filepaths),
                    len(scenarios_for_comparison) if minmax_mode else len(scenarios),
                    len(scenarios) if minmax_mode else len(scenarios),
                    f"{total_orders_processed:,}",
                    f"{processing_time:.2f}",
                    f"{orders_per_second:.1f}",
                    f"{processing_time/len(scenarios_for_comparison):.2f}" if minmax_mode else f"{processing_time/len(scenarios):.2f}",
                    "; ".join([os.path.basename(f) for f in filepaths])
                ]
            })
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
        
        # Display results
        if minmax_mode:
            # Min/Max optimization summary
            files_processed = list(set([s['filepath'] for s in scenarios_for_comparison]))
            
            summary_text = f"""🔥 MIN/MAX OPTIMIZATION COMPLETE!

📊 OPTIMIZATION ANALYSIS:
   Files Analyzed: {len(files_processed)}
   Sorting Strategies Tested: {len(get_sorting_strategies())}
   Total Strategy Tests: {len(scenarios_for_comparison)}
   Best Strategies Saved: {len(scenarios)} individual sheets (3 per file: Orders, Hours, Qty)

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
      → {best_orders['metrics']['releasable_count']:,}/{best_orders['metrics']['total_orders']:,} orders releasable ({best_orders['metrics']['releasable_count']/best_orders['metrics']['total_orders']*100:.1f}%)
   
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
   Processing Speed: {orders_per_second:.1f} orders/second
   Average per Strategy: {processing_time/len(scenarios_for_comparison):.1f} seconds
   
💾 Results saved to:
   {os.path.basename(output_file)}
   
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

🏆 BEST PERFORMER: {os.path.basename(best_scenario['filepath'])}
   ✅ {best_scenario['metrics']['releasable_count']:,} releasable orders ({best_scenario['metrics']['releasable_count']/best_scenario['metrics']['total_orders']*100:.1f}%)

📉 BASELINE: {os.path.basename(worst_scenario['filepath'])}
   ✅ {worst_scenario['metrics']['releasable_count']:,} releasable orders ({worst_scenario['metrics']['releasable_count']/worst_scenario['metrics']['total_orders']*100:.1f}%)

🔺 IMPROVEMENT: +{improvement:,} more orders releasable

⏱️ PERFORMANCE METRICS:
   Total Processing Time: {processing_time:.2f} seconds
   Processing Speed: {orders_per_second:.1f} orders/second
   
💾 Results saved to: {os.path.basename(output_file)}"""
        else:
            # Single scenario summary
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

⏱️ PERFORMANCE METRICS:
   Processing Time: {processing_time:.2f} seconds
   Orders per Second: {orders_per_second:.1f}

💾 Results saved to: {os.path.basename(output_file)}"""
        
        results_text.delete(1.0, tk.END)
        results_text.insert(1.0, summary_text)
        
        # For status bar
        if minmax_mode:
            status_var.set(f"🔥 MIN/MAX OPTIMIZATION COMPLETE! {len(scenarios_for_comparison)} strategies tested, {len(scenarios)} best results saved in {processing_time:.1f}s")
        elif len(scenarios) > 1:
            best_scenario = max(scenarios, key=lambda s: s['metrics']['releasable_count'])
            worst_scenario = min(scenarios, key=lambda s: s['metrics']['releasable_count'])
            improvement = best_scenario['metrics']['releasable_count'] - worst_scenario['metrics']['releasable_count']
            status_var.set(f"✅ ALL {len(scenarios)} SCENARIOS COMPLETE! Best: {best_scenario['metrics']['releasable_count']:,} releasable (+{improvement:,} vs worst) | Total time: {processing_time:.1f}s")
        else:
            total_orders = scenarios[0]['metrics']['total_orders']
            total_releasable = scenarios[0]['metrics']['releasable_count']
            status_var.set(f"✅ PROCESSING COMPLETE! {total_releasable:,}/{total_orders:,} orders releasable in {processing_time:.1f}s")
            
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
root.title(f"PlanSnap {VERSION}")
root.geometry("750x800")

# Main frame
main_frame = ttk.Frame(root, padding="20")
main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

# Title
title_label = ttk.Label(main_frame, text=f"PlanSnap {VERSION}", 
                       font=('Arial', 16, 'bold'))
title_label.grid(row=0, column=0, pady=(0, 20))

# Instructions
instructions = """Lightning-fast material planning with multi-scenario analysis and optimization!

NEW IN v1.8.0 - TRIPLE OPTIMIZATION MODE:
🔥 Now optimizes for THREE different objectives and saves the best strategy for each!
• 🏆 BEST ORDERS: Maximum number of orders releasable
• 🏆 BEST HOURS: Maximum labor hours releasable  
• 🏆 BEST QTY: Maximum total quantity (units) releasable

Tests 10 different sorting strategies to find optimal material utilization for each objective!

SORTING STRATEGIES TESTED:
• Start Date (Early/Late First) • Demand Qty (Small/Large First) • Labor Hours (Quick/Long First)
• Part Number (A-Z/Z-A) • Planner (A-Z/Z-A)

Each strategy processes orders in different sequence → different optimization results!
Perfect for answering: "Should I prioritize order count, labor efficiency, or total throughput?"

STANDARD FEATURES:
• Select MULTIPLE files to compare different scenarios (hold Ctrl)
• Real-time progress tracking with scenario context
• All-or-nothing material allocation for accuracy
• Detailed shortage analysis with PO information
• Comprehensive Excel reporting with comparison tables

Click the checkbox below to enable Min/Max Optimization Mode, then select your files."""

inst_label = ttk.Label(main_frame, text=instructions, justify=tk.LEFT, wraplength=700)
inst_label.grid(row=1, column=0, pady=(0, 20))

# Min/Max Mode checkbox
minmax_frame = ttk.Frame(main_frame)
minmax_frame.grid(row=2, column=0, pady=(0, 20))

minmax_var = tk.BooleanVar()
minmax_checkbox = ttk.Checkbutton(
    minmax_frame, 
    text="🔥 Enable Triple Optimization Mode (finds best strategies for Orders + Hours + Qty)",
    variable=minmax_var,
    style='Big.TCheckbutton'
)
minmax_checkbox.grid(row=0, column=0)

# Process button
process_btn = ttk.Button(main_frame, text="📂 SELECT FILES & PROCESS", 
                        command=load_and_process_files, 
                        style='Big.TButton')
process_btn.grid(row=3, column=0, pady=(0, 20))

# Configure button style
style = ttk.Style()
style.configure('Big.TButton', font=('Arial', 12, 'bold'))
style.configure('Big.TCheckbutton', font=('Arial', 10, 'bold'))
style.configure('Success.TFrame', background='#7ff09a')

# Status
status_var = tk.StringVar()
status_var.set("🔄 Ready - Select Excel file(s) to begin processing")
status_label = ttk.Label(main_frame, textvariable=status_var, font=('Arial', 10))
status_label.grid(row=4, column=0, pady=(0, 10), sticky=tk.W)

# Results area
results_frame = ttk.LabelFrame(main_frame, text="📊 Results", padding="10")
results_frame.grid(row=5, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))

results_text = tk.Text(results_frame, height=18, width=90, font=('Consolas', 9))
scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=results_text.yview)
results_text.configure(yscrollcommand=scrollbar.set)

results_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

copy_btn = ttk.Button(main_frame, text="📋 Copy Summary", command=copy_summary_to_clipboard)
copy_btn.grid(row=6, column=0, pady=(10, 10))

# Configure grid weights
root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)
main_frame.columnconfigure(0, weight=1)
main_frame.rowconfigure(5, weight=1)
results_frame.columnconfigure(0, weight=1)
results_frame.rowconfigure(0, weight=1)

# Show initial message
results_text.insert(1.0, "Select your Excel file(s) to begin material release planning...\n\nNEW in v1.7.1:\n🔥 TRIPLE OPTIMIZATION MODE!\n• 🏆 BEST ORDERS: Max orders releasable\n• 🏆 BEST HOURS: Max labor hours releasable\n• 🏆 BEST QTY: Max total quantity releasable\n• Tests 10 sorting strategies per file\n• Saves 3 optimal results per file\n\nCheck the box above to enable optimization mode!")

if __name__ == "__main__":
    root.mainloop()