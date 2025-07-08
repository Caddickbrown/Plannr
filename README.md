# PlanSnap v2.0.0 - Material Release Planning Tool (SQL Server Version)

## Overview
PlanSnap is a material release planning tool that analyzes shop orders and determines which orders can be released based on material availability. This version integrates directly with SQL Server databases instead of Excel files.

## Features
- **SQL Server Integration**: Direct connection to SQL Server databases
- **Triple Optimization Mode**: Tests all sorting strategies to find optimal results for Orders, Hours, and Quantity
- **Material Category Filtering**: Process specific categories (Kits, Instruments, Virtuoso, Kit Samples)
- **Real-time Processing**: Live progress updates during analysis
- **Excel Export**: Generate detailed reports with multiple sheets

## Database Setup

### 1. Configure Database Credentials
Edit `db_credentials.env` with your SQL Server connection details:

```env
DB_HOST=your_server_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database
DB_PORT=1433
```

### 2. Required Database Tables
The application expects the following tables in your database:

#### Demand Table (`demand_table`)
```sql
CREATE TABLE demand_table (
    `SO No` VARCHAR(50),
    `Part No` VARCHAR(50),
    `Rev Qty Due` DECIMAL(10,2),
    `Start Date` DATE,
    `Planner` VARCHAR(20)
);
```

#### Planned Demand Table (`planned_demand_table`)
```sql
CREATE TABLE planned_demand_table (
    `SO Number` VARCHAR(50),
    `Component Part Number` VARCHAR(50),
    `Component Qty Required` DECIMAL(10,2)
);
```

#### Component Demand Table (`component_demand_table`)
```sql
CREATE TABLE component_demand_table (
    `Component Part Number` VARCHAR(50),
    `Component Qty Required` DECIMAL(10,2)
);
```

#### IPIS Table (`ipis_table`)
```sql
CREATE TABLE ipis_table (
    `PART_NO` VARCHAR(50),
    `Available Qty` DECIMAL(10,2)
);
```

#### Hours Table (`hours_table`)
```sql
CREATE TABLE hours_table (
    `PART_NO` VARCHAR(50),
    `Hours per Unit` DECIMAL(10,4)
);
```

#### POs Table (`pos_table`)
```sql
CREATE TABLE pos_table (
    `PO Number` VARCHAR(50),
    `Part Number` VARCHAR(50),
    `Qty Due` DECIMAL(10,2),
    `Promised Due Date` DATE
);
```

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your database credentials in `db_credentials.env`

3. Ensure your database tables are set up with the correct structure

## Usage

1. Run the application:
```bash
python AMCBDG_SQL.py
```

2. Configure processing options:
   - **Triple Optimization Mode**: Enable to test all sorting strategies
   - **Quick Analysis Mode**: Disable Excel export for faster results
   - **Material Categories**: Select which categories to process

3. Click "🗄️ CONNECT TO DATABASE & PROCESS" to begin analysis

## Output

The tool generates:
- **Summary Sheet**: Overall metrics and performance statistics
- **Strategy Comparison**: Results from all tested sorting strategies (if optimization mode enabled)
- **Individual Scenario Sheets**: Detailed results for each scenario

## Material Categories

- **Kits** (Planner codes: 3001, 3801, 5001)
- **Instruments** (Planner codes: 3802, 3803, 3804, 3805)
- **Virtuoso** (Planner code: 3806)
- **Kit Samples** (Planner code: KIT SAMPLES)

## Database Support

- **SQL Server**: Full support with pymssql

## Troubleshooting

### Connection Issues
- Verify database credentials in `db_credentials.env`
- Ensure database server is running
- Check firewall settings for remote connections

### Missing Tables
- Create required tables with correct column names
- Ensure data types match expected format
- Verify table permissions for your database user

### Performance Issues
- Use Quick Analysis Mode for faster results
- Consider indexing frequently queried columns
- Optimize database queries for large datasets

## Version History

- **v2.0.0**: SQL Server database integration
- **v1.9.0**: Excel-based processing with optimization features
