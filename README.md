# Expense Data Warehouse ETL Pipeline

An end-to-end Python + SQL ETL pipeline that processes raw expense CSV files, performs data cleaning and validation, loads data into a MySQL Data Warehouse, and supports both Full Load and Incremental Load processing with Slowly Changing Dimension Type 2 (SCD Type 2).

## Project Overview

This project demonstrates how raw expense data can be transformed into a structured data warehouse using a Python-based ETL pipeline.

- Raw CSV ingestion
- Data cleaning and transformation
- Data validation
- Full load processing
- Incremental load processing
- SCD Type 2 for employee changes
- Dimension and fact table creation
- Primary and foreign keys
- Surrogate keys using AUTO_INCREMENT
- Detection of new records
- Detection of changed records
- MySQL data warehouse loading
- Logging
- Error handling
- Raw file archiving
- Configuration management using `.env` and `config.py`


## Architecture
```
Raw Expense CSV Files
        │
        ▼
   Python / Pandas
        │
        ▼
Data Cleaning & Validation
        │
        ▼
Check Database Structure
        │
        ├───────────────┐
        │               │
   First Run       Existing DB
        │               │
        ▼               ▼
   FULL LOAD     INCREMENTAL LOAD
        │               │
        │          SCD Type 2
        │               │
        └───────┬───────┘
                │
                ▼
        Dimension Tables
                │
                ▼
           FactExpense
                │
                ▼
      MySQL Data Warehouse
            
```

## Data Warehouse Model

The project uses a dimensional modelling / star schema approach.

- Dimension Tables

1. DimDate

Stores date-related information.

DateID
ExpenseDate
Month
Year

2. DimEmployee

Stores employee information and historical versions.

employeeID
EmployeeName
City
EffectiveDate
EndDate
IsActive

The DimEmployee table uses SCD Type 2 to preserve historical changes such as an employee changing cities.

3. DimDepartment

Stores department information.

DeptID
Departmentname

4. Fact Table

FactExpense

Stores expense transactions.

ExpenseID
DateID
employeeID
DeptID
ExpenseType
Vendor
PaymentMode
Amount

The fact table contains foreign keys connecting expense transactions to the dimension tables.

The database tables are created with primary keys, foreign keys, and auto-incrementing surrogate keys.

## ETL Process

1. Extract

The pipeline reads CSV files from:

Data/Raw/

Multiple CSV files can be processed.

After processing, the raw files are moved to:

Data/Archive/

This prevents already-processed files from remaining in the raw input directory.

2. Transform

The pipeline performs several transformations:

Date conversion
Duplicate removal
Employee name standardization
City standardization
Dimension creation
Surrogate-key mapping
Fact-table preparation

3. Validate

The pipeline validates incoming data before loading it into MySQL.

Validation includes:

Empty dataset check
NULL ExpenseID
Duplicate ExpenseID
NULL Amount
Negative Amount
Invalid ExpenseDate

Separate validation functions are used for full and incremental loads.

## Full Load

The full-load process:

- Creates the warehouse tables.
- Reads raw CSV files.
- Cleans the data.
- Validates the data.
- Creates dimension data.
- Loads dimension tables.
- Reads generated surrogate keys.
- Builds the fact table.
- Loads FactExpense.

## Incremental Load

Incremental-load process:

- Reads new CSV files.
- Cleans the incoming data.
- Validates the incremental batch.
- Reads existing employee dimension data.
- Detects employee changes and applies SCD Type 2.
- Inserts new departments and dates into the dimension tables.
- Reads the updated dimension tables and generated surrogate keys.
- Identifies new expense records and inserts them into FactExpense.
- Checks existing expense records for changes.
- Updates changed fact records and skips unchanged records.

After the warehouse has been created, subsequent files are processed using Incremental Load.

## SCD Type 2

Initially:
```
Employee   City      EffectiveDate   EndDate   IsActive
--------------------------------------------------------
Rahul      Mumbai    2026-01-01      NULL      True
```

If Rahul changes city to Pune:
```
Employee   City      EffectiveDate   EndDate      IsActive
----------------------------------------------------------
Rahul      Mumbai    2026-01-01      2026-06-14   False
Rahul      Pune      2026-06-15      NULL         True
```

Instead of overwriting the old record, the pipeline:

1. Finds the active employee record.
2. Checks whether the employee information changed.
3. Closes the old record.
4. Sets IsActive = FALSE.
5. Sets the EndDate.
6. Inserts a new version.
7. Sets the new version as active.

This preserves historical employee information.

## Incremental Fact Processing

The pipeline also checks whether an incoming ExpenseID already exists in FactExpense.

1. New Expense

If the ExpenseID does not exist:
```
New Expense
     ↓
Insert into FactExpense
```

2. Existing Expense

If the ExpenseID already exists, the pipeline compares:

- Date
- Employee
- Department
- Expense Type
- Vendor
- Payment Mode
- Amount

If any value has changed:
```
Existing Expense
       ↓
Compare with Database
       ↓
Changed?
   /       \
 Yes        No
  ↓          ↓
Update     Skip
```

This prevents unnecessary inserts and updates.

## Logging

The pipeline uses Python's built-in logging module.

Logs are written to:

etl.log

and are also displayed in the terminal.

Example:

- ETL STARTED
- Reading input CSV file
- Data cleaning completed
- Data validation completed
- Dimension tables loaded
- INCREMENTAL LOAD COMPLETED SUCCESSFULLY

The logging configuration uses both:

- FileHandler
- StreamHandler

## Error Handling

The full-load and incremental-load processes are wrapped in try/except blocks.

If an ETL process fails, the exception is captured using:

logging.exception()

This provides error information in the log file and helps with troubleshooting.


## Project Structure
```
Expense-Data-Warehouse-ETL/
│
├── Data/
│   ├── Raw/
│   └── Archive/
│
├── config.py
├── .env
├── etl.py
├── etl.log
├── requirements.txt
├── README.md
├── license
└── .gitignore
```

## Technologies Used
- Python
- Pandas
- SQLAlchemy
- PyMySQL
- MySQL
- SQL
- Logging
- CSV
- Git
- GitHub

## Data Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- ETL pipeline development
- Data extraction
- Data cleaning
- Data transformation
- Data validation
- Dimensional modelling
- Star schema
- Fact tables
- Dimension tables
- Primary keys
- Foreign keys
- Surrogate keys
- Full loading
- Incremental loading
- Slowly Changing Dimension Type 2
- Historical data management
- Data quality checks
- Error handling
- Logging
- Raw-file archiving

## How to Run

1. Install Dependencies
pip install pandas sqlalchemy pymysql python-dotenv

2. Configure Database

Database credentials are stored separately from the ETL code.

Example:

DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_NAME=Expense

3. Add Raw CSV Files

Place your input CSV files inside:

Data/Raw/

4. Run the Pipeline
python etl.py

The pipeline automatically determines whether the process should be a Full Load or Incremental Load.

## Example Workflow

1. First Run
```
CSV Batch 1
     │
     ▼
FactExpense Does Not Exist
     │
     ▼
FULL LOAD
     │
     ▼
Create Dimensions
     │
     ▼
Create Fact
     │
     ▼
MySQL Data Warehouse
```

2. Later Run
```
CSV Batch 2
     │
     ▼
FactExpense Already Exists
     │
     ▼
INCREMENTAL LOAD
     │
     ├── SCD Type 2
     │
     ├── New Dimensions
     │
     ├── New Facts
     │
     └── Changed Facts
     │
     ▼
MySQL Data Warehouse
```

## Future Enhancements

Possible future improvements include:

- FastAPI data-access layer
- Docker containerization
- More advanced data-quality checks

## Author
**Akshay Gawand**


