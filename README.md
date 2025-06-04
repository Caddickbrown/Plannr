# Plannr

## Overview
Plannr is a suite of production planning and scheduling tools designed to optimize manufacturing operations. It includes multiple modules for materials checking, scheduling, and production optimization.

## Features
- Material Requirements Planning
- Production Scheduling
- Capacity Planning
- Multi-Department Coordination
- Constraint-Based Optimization
- Excel Integration

## Modules

### AMCBD (Automated Materials Checker) (Beta)
An automated materials checker with min/max optimization capabilities:
- Material availability analysis
- Stock level optimization
- Multiple metric focus areas
- Comparative scenario analysis
- Customizable sorting strategies
- Stock dictionary building

#### Required Input Files:
1. **Main Sheet:**
   - Part: Part number (string)
   - SO Number: Shop order number
   - Start Date: Order start date
   - Demand: Required quantity
   - Planner: Planner name/ID

2. **Planned Demand Sheet:**
   - SO Number: Shop order reference
   - Component Part Number: Required parts
   - Component quantities

3. **Component Demand Sheet:**
   - Component Part Number
   - Component Qty Required

4. **IPIS Sheet:**
   - PART_NO: Part number
   - Available Qty: Current stock level

5. **Hours Sheet:**
   - PART_NO: Part number
   - Hours per Unit: Labor standard

6. **POs Sheet:**
   - Purchase order information
   - Expected deliveries

#### Sorting Strategies:
- Start Date (Early/Late First)
- Demand (Small/Large First)
- Hours (Quick/Long First)
- Part Number (A-Z/Z-A)
- Planner (A-Z/Z-A)

### AMCSummareyes (Stable)
A companion tool to AMCBD that provides:
- Detailed summary reports
- Data visualization
- Metric aggregation
- Performance analytics
- Export capabilities

### InstSch (Instruments Scheduler)
A specialized scheduler for instrument production (Beta):
- Department-specific scheduling
- Instrument-specific constraints
- Multi-department coordination
- Format batching support

### SchEng (Scheduling Engine)
A flexible constraint-based scheduling engine that optimizes production schedules based on:
- Resource capacity
- Labor hours
- Material availability
- Due dates
- Department constraints

#### Key Features:
- Multi-department scheduling
- Flexible constraint system
- Priority-based scheduling
- Real-time optimization
- Excel import/export
- Interactive GUI

#### Required Input Files:
1. **ReleasedPOOL Sheet:**
   - Order information (Order No, Part No, Quantity, Dates)
   - Production requirements

2. **Main Sheet:**
   - Additional order details
   - Resource requirements (Picks, Hours, Boxes)
   - Geographic and brand information

## Installation

### Prerequisites
1. Python 3.7 or higher
2. Required Python packages:
   ```
   pandas>=1.3.0
   numpy>=1.20.0
   openpyxl>=3.0.0
   ```

### Setup Steps
1. Download the Plannr suite to your local machine
2. Install required dependencies:
   ```bash
   pip install pandas numpy openpyxl
   ```
3. Ensure Excel is installed for file operations

## Usage

### Running AMCBD
1. Prepare your Excel file with required sheets:
   - Main: Order details and demands
   - Planned Demand: Component requirements
   - Component Demand: Current commitments
   - IPIS: Stock levels
   - Hours: Labor standards
   - POs: Purchase order data

2. Launch the application:
   ```bash
   python AMCBD.py
   ```

3. Use the interface to:
   - Select input file(s)
   - Choose sorting strategy
   - Run analysis
   - View results
   - Export reports

4. Analysis Features:
   - Stock availability check
   - Component commitment tracking
   - Labor hours calculation
   - Multiple scenario comparison
   - Progress tracking with time estimates

### Running the Scheduling Engine (SchEng)
1. Prepare your input Excel file with required sheets:
   - ReleasedPOOL
   - Main
   - Hours (optional)
2. Launch the application:
   ```bash
   python SchEng.py
   ```
3. Use the GUI to:
   - Load your planning data
   - Set constraints
   - Generate schedules
   - Export results

### Data File Requirements
- **Format**: Excel (.xlsx or .xlsm)
- **Required Sheets**:
  - ReleasedPOOL: Order details
  - Main: Additional information
  - IPIS: Stock levels (for AMCBD)
- **Optional Sheets**:
  - Hours: Labor standards
  - Constraints: Capacity limits

## Configuration
- Adjust department capacities
- Set scheduling constraints
- Modify priority calculations
- Configure output formats
- Customize sorting strategies (AMCBD)
- Set metric thresholds

## Troubleshooting
Common issues and solutions:
1. **Excel File Access**: 
   - Ensure file is not open in Excel
   - Check file permissions
2. **Missing Data**: 
   - Verify all required sheets exist
   - Check column names match expected format
3. **Format Issues**: 
   - Check date formats (YYYY-MM-DD)
   - Verify numeric data types
4. **Performance**: 
   - Close other applications for large datasets
   - Consider splitting large files
5. **GUI Issues**:
   - Verify tkinter installation
   - Check screen resolution settings

## Support
Contact the Production Control Department for support and feature requests.