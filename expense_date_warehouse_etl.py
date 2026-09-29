import pandas as pd
from sqlalchemy import create_engine, text, inspect
import glob
import shutil
import os
import logging 
from sqlalchemy import create_engine
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME

logging.basicConfig(
    level =logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    handlers =[logging.FileHandler("etl.log"),
               logging.StreamHandler()]
)

## MYSQL CONNECTIOn ## setting tables

def get_engine():

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    )
    with engine.begin() as conn:

        conn.execute(text("DROP TABLE IF EXISTS FactExpense"))
        conn.execute(text("DROP TABLE IF EXISTS Dimemployee"))
        conn.execute(text("DROP TABLE IF EXISTS Dimdepartment"))
        conn.execute(text("DROP TABLE IF EXISTS Dimdate"))
    return engine
 
def create_pk_fk_scd2(engine):
# creating tables pk, fk, autoincrement, scd 2

    with engine.begin() as conn:

        conn.execute(text("""
            CREATE TABLE Dimdate (
                DateID INT AUTO_INCREMENT PRIMARY KEY,
                ExpenseDate DATE NOT NULL,
                Month INT,
                Year INT
            )
        """))

        conn.execute(text("""
            CREATE TABLE DimEmployee (
                employeeID INT AUTO_INCREMENT PRIMARY KEY,
                EmployeeName VARCHAR(100) NOT NULL,
                City VARCHAR(100),
                EffectiveDate DATE,
                EndDate DATE,
                IsActive BOOLEAN
            )
        """))

        conn.execute(text("""
            CREATE TABLE DimDepartment (
                DeptID INT AUTO_INCREMENT PRIMARY KEY,
                Departmentname VARCHAR(100) NOT NULL
            )
        """))

        conn.execute(text("""
            CREATE TABLE Factexpense (
                ExpenseID INT PRIMARY KEY,
                DateID INT NOT NULL,
                employeeID INT NOT NULL,
                DeptID INT NOT NULL,
                ExpenseType VARCHAR(100),
                Vendor VARCHAR(100),
                PaymentMode VARCHAR(50),
                Amount DECIMAL(12,2),

                FOREIGN KEY (DateID)
                    REFERENCES Dimdate(DateID),

                FOREIGN KEY (employeeID)
                    REFERENCES DimEmployee(employeeID),

                FOREIGN KEY (DeptID)
                    REFERENCES DimDepartment(DeptID)
            )
        """))


def clean_data():

    logging.info("Reading input CSV file")

    dfs = []

    ## reading and cleaning data

    os.makedirs("Data/Raw", exist_ok  = True)

    os.makedirs("Data/Archive", exist_ok=True)
    
    files = glob.glob("Data/Raw/*.csv")

    for file in files:

        df = pd.read_csv(file)

        df["ExpenseDate"] = pd.to_datetime(
        df["ExpenseDate"],
        errors="coerce"
            ).dt.normalize()
        df.drop_duplicates(inplace=True)

        df["Employee"] = (
            df["Employee"]
            .str.strip()
            .str.title()
        )

        df["City"] = (
            df["City"]
            .str.strip()
            .str.title()
        )
        logging.info("Data cleaning completed")

        dfs.append(df)

        shutil.move(file, "Data/Archive")

    df = pd.concat(dfs, ignore_index=True)

    return df
## validate full load

def validate_full_load(df):

    logging.info("Data validation started")

    if df.empty:
        raise ValueError("Full load failed: input data is empty")
    
    if df["ExpenseID"].isnull().any():
        raise ValueError("Full load failed: ExpenseID contains NULL")    

    if df["ExpenseID"].duplicated().any():
        raise ValueError("Full load failed: duplicate ExpenseID found")

    if df["Amount"].isnull().any():
        raise ValueError("Full load failed: Amount contains NULL")

    if (df["Amount"] < 0).any():
        raise ValueError("Full load failed: negative Amount found")

    if df["ExpenseDate"].isnull().any():
        raise ValueError("Full load failed: invalid ExpenseDate")

    logging.info("Data validation completed")


def create_dim_data(df):

    logging.info("Creating dimension tables")
    ## creating employee data

     ## creating date data
    
    dim_date = (
            df[["ExpenseDate"]]
            .drop_duplicates()
            .sort_values("ExpenseDate")
            .reset_index(drop=True)
        )
    
    dim_date["Month"] = dim_date["ExpenseDate"].dt.month
    dim_date["Year"] = dim_date["ExpenseDate"].dt.year


    dim_employee = (
        df[["Employee", "City"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_employee = dim_employee.rename(
        columns={
            "Employee": "EmployeeName"
        }
    )

    dim_employee["EffectiveDate"] = (
        df.groupby("Employee")["ExpenseDate"]
        .min()
        .reindex(dim_employee["EmployeeName"])
        .values
    )

    dim_employee["EndDate"] = None
    dim_employee["IsActive"] = True

    ## creating dept data

    dim_department = (
        df[["Department"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_department = dim_department.rename(
        columns={
            "Department": "Departmentname"
        }
    )
    return dim_date, dim_department, dim_employee

def load_data(dim_date, dim_department, dim_employee, engine):

    ## inserting data in mysql and looking for ids

    dim_date.to_sql(
        "Dimdate",
        con=engine,
        if_exists="append",
        index=False
    )

    dim_employee.to_sql(
        "DimEmployee",
        con=engine,
        if_exists="append",
        index=False
    )

    dim_department.to_sql(
        "DimDepartment",
        con=engine,
        if_exists="append",
        index=False
    )
    logging.info("dimension tables loaded")

def read_data(engine):

    ## reading generated data from sql

    dim_date_db = pd.read_sql(
        "SELECT * FROM Dimdate",
        engine
    )

    dim_date_db["ExpenseDate"] = pd.to_datetime(
        dim_date_db["ExpenseDate"],
        errors="coerce"
    )

    dim_employee_db = pd.read_sql(
        "SELECT * FROM DimEmployee",
        engine
    )

    dim_department_db = pd.read_sql(
        "SELECT * FROM DimDepartment",
        engine
    )
    return dim_department_db, dim_employee_db, dim_date_db

def fact_build_load(df, dim_department_db, dim_employee_db, dim_date_db, engine):
    ## buiding fact table

    logging.info("creating FactExpense")

    fact = df.merge(
        dim_date_db[["DateID", "ExpenseDate"]],
        on="ExpenseDate",
        how="left"
    )

    fact = fact.merge(
        dim_employee_db[["employeeID", "EmployeeName"]],
        left_on="Employee",
        right_on="EmployeeName",
        how="left"
    )

    fact = fact.merge(
        dim_department_db[["DeptID", "Departmentname"]],
        left_on="Department",
        right_on="Departmentname",
        how="left"
    )


    fact_expense = fact[
        [
            "ExpenseID",
            "DateID",
            "employeeID",
            "DeptID",
            "ExpenseType",
            "Vendor",
            "PaymentMode",
            "Amount"
        ]
    ]

    ## inserting table in sql

    fact_expense.to_sql(
        "Factexpense",
        con=engine,
        if_exists="append",
        index=False
    )

    logging.info("factexpense created.")

    logging.info("Batch 1 loaded successfully.")



def get2_engine():

    ## MYSQL CONNECTION

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    )
    return engine

def validate_incremental_load(df):

    logging.info("Data validation started")

    if df.empty:
        raise ValueError("Incremental load failed: batch is empty")

    if df["ExpenseID"].isnull().any():
        raise ValueError("Incremental load failed: ExpenseID contains NULL")

    if df["ExpenseID"].duplicated().any():
        raise ValueError("Incremental load failed: duplicate ExpenseID in batch")

    if df["Amount"].isnull().any():
        raise ValueError("Incremental load failed: Amount contains NULL")

    if (df["Amount"] < 0).any():
        raise ValueError("Incremental load failed: negative Amount found")

    if df["ExpenseDate"].isnull().any():
        raise ValueError("Incremental load failed: invalid ExpenseDate")

    logging.info("Data validation completed")
    

def read_data2(engine):

        ## READ EXISTING DIMENSIONS


    dim_employee_db = pd.read_sql(
            "SELECT * FROM DimEmployee",
            engine
        )

    return dim_employee_db

def create_sd2_dim(df, engine, dim_employee_db):

    df = df.sort_values( ["Employee", "ExpenseDate"] ).reset_index(drop=True)
    ## scd 2 employee dimension creation

    for _, row in df.iterrows():

        employee_name = row["Employee"]
        new_city = row["City"]
        change_date = row["ExpenseDate"]


        ## finding active_employee

        active_employee = dim_employee_db[
            (dim_employee_db["EmployeeName"] == employee_name)
            &
            (dim_employee_db["IsActive"] == True)
        ]

    ## if employee not exist

        if active_employee.empty:

            with engine.begin() as conn:

                conn.execute(
                    text("""
                        INSERT INTO DimEmployee
                        (
                            EmployeeName,
                            City,
                            EffectiveDate,
                            EndDate,
                            IsActive
                        )
                        VALUES
                        (
                            :EmployeeName,
                            :City,
                            :EffectiveDate,
                            NULL,
                            TRUE
                        )
                    """),
                    {
                        "EmployeeName": employee_name,
                        "City": new_city,
                        "EffectiveDate": change_date
                    }
                )

            print(
                f"Inserted new employee {employee_name}"
            )
            dim_employee_db = pd.read_sql(
            "SELECT * FROM DimEmployee",
            engine
        )

        ## if employee already exist

        else:

            old_row = active_employee.iloc[0]

            old_city = old_row["City"]

            ## same city no dimenstion change

            if old_city == new_city:

                print(
                    f"Skipped employee {employee_name} - "
                    f"no SCD2 change"
                )
                dim_employee_db = pd.read_sql(
                "SELECT * FROM DimEmployee",
                engine
                )

            elif change_date <= pd.Timestamp(old_row["EffectiveDate"]):
                print(
                f"Skipped {employee_name} - "
                f"change date {change_date} is earlier than current version"
            )
                continue

            ## city changed

            else:

                employee_id = old_row["employeeID"]

            ## closing old version

                end_date = change_date - pd.Timedelta(days=1)

                with engine.begin() as conn:

                    conn.execute(
                        text("""
                            UPDATE DimEmployee
                            SET
                                EndDate = :EndDate,
                                IsActive = FALSE
                            WHERE employeeID = :employeeID
                        """),
                        {
                            "EndDate": end_date,
                            "employeeID": employee_id
                        }
                    )

                ## inserting new version

                    conn.execute(
                        text("""
                            INSERT INTO DimEmployee
                            (
                                EmployeeName,
                                City,
                                EffectiveDate,
                                EndDate,
                                IsActive
                            )
                            VALUES
                            (
                                :EmployeeName,
                                :City,
                                :EffectiveDate,
                                NULL,
                                TRUE
                            )
                        """),
                        {
                            "EmployeeName": employee_name,
                            "City": new_city,
                            "EffectiveDate": change_date
                        }
                    )

                print(
                    f"SCD2 change for {employee_name}: "
                    f"{old_city} -> {new_city}"
                )

                dim_employee_db = pd.read_sql(
                    "SELECT * FROM DimEmployee",
                    engine)
    return dim_employee_db
 
def find_insert_new_dim(engine, df):                
    ## finding new department

    dim_department_db = pd.read_sql(
        "SELECT * FROM DimDepartment",
        engine
    )

    existing_departments = set(
        dim_department_db["Departmentname"]
    )

    new_departments = df[
        ~df["Department"].isin(existing_departments)
    ][
        ["Department"]
    ].drop_duplicates()

    new_departments = new_departments.rename(
        columns={
            "Department": "Departmentname"
        }
    )
    ## inserting new departments

    if not new_departments.empty:

        new_departments.to_sql(
            "DimDepartment",
            con=engine,
            if_exists="append",
            index=False
        )

        print("New departments inserted:")
        print(new_departments)

    ## finding new dats

    dim_date_db = pd.read_sql(
        "SELECT * FROM Dimdate",
        engine
    )

    existing_dates = set(
        pd.to_datetime(
            dim_date_db["ExpenseDate"]
        )
    )

    new_dates = df[
        ~df["ExpenseDate"].isin(existing_dates)
    ][
        ["ExpenseDate"]
    ].drop_duplicates()

    new_dates["Month"] = (
        new_dates["ExpenseDate"].dt.month
    )

    new_dates["Year"] = (
        new_dates["ExpenseDate"].dt.year
    )
    ## inserting new dates

    if not new_dates.empty:

        new_dates.to_sql(
            "Dimdate",
            con=engine,
            if_exists="append",
            index=False
        )

        print("New dates inserted:")
        print(new_dates)

    ## reading dimentions again

    dim_employee_db = pd.read_sql(
        "SELECT * FROM DimEmployee",
        engine
    )

    dim_department_db = pd.read_sql(
        "SELECT * FROM DimDepartment",
        engine
    )

    dim_date_db = pd.read_sql(
        "SELECT * FROM Dimdate",
        engine
    )

    dim_date_db["ExpenseDate"] = pd.to_datetime(dim_date_db["ExpenseDate"], errors  ="coerce")

    logging.info("Tables created/verified")
    
    return dim_department_db, dim_employee_db, dim_date_db



def reading_inserting_n_facts(engine, df, dim_department_db,  dim_employee_db, dim_date_db):


    ## reading existing fact

    fact_db = pd.read_sql(
        "SELECT * FROM Factexpense",
        engine
    )
    ## buiding fact

    fact = df.merge(
        dim_date_db[
            ["DateID", "ExpenseDate"]
        ],
        on="ExpenseDate",
        how="left"
    )
    ## using active_employee version

    active_employees = dim_employee_db[
        dim_employee_db["IsActive"] == True
    ][
        ["employeeID", "EmployeeName"]
    ]

    fact = fact.merge(
        active_employees,
        left_on="Employee",
        right_on="EmployeeName",
        how="left"
    )

    fact = fact.merge(
        dim_department_db[
            ["DeptID", "Departmentname"]
        ],
        left_on="Department",
        right_on="Departmentname",
        how="left"
    )
    ## fact columns selection

    fact_expense = fact[
        [
            "ExpenseID",
            "DateID",
            "employeeID",
            "DeptID",
            "ExpenseType",
            "Vendor",
            "PaymentMode",
            "Amount"
        ]
    ]
    ## finding new expense

    existing_expense_ids = set(
        fact_db["ExpenseID"]
    )

    new_facts = fact_expense[
        ~fact_expense["ExpenseID"].isin(
            existing_expense_ids
        )
    ]

    ## inserting new factexpense

    if not new_facts.empty:

        new_facts.to_sql(
            "Factexpense",
            con=engine,
            if_exists="append",
            index=False
        )

        print("\nNew facts inserted:")

        print(new_facts)


    return fact_expense, existing_expense_ids, fact_db

def changing_existing_facts(fact_expense, existing_expense_ids, engine, fact_db):

    ## finding existind expesne

    existing_facts = fact_expense[

        fact_expense["ExpenseID"].isin(
            existing_expense_ids
        )
    ]

    ## checking existing facts

    for _, row in existing_facts.iterrows():

        expense_id = row["ExpenseID"]

        old_row = fact_db[
            fact_db["ExpenseID"] == expense_id
        ].iloc[0]

    ##comaparing values

        changed = (
            old_row["DateID"] != row["DateID"]
            or
            old_row["employeeID"] != row["employeeID"]
            or
            old_row["DeptID"] != row["DeptID"]
            or
            old_row["ExpenseType"] != row["ExpenseType"]
            or
            old_row["Vendor"] != row["Vendor"]
            or
            old_row["PaymentMode"] != row["PaymentMode"]
            or
            float(old_row["Amount"]) != float(row["Amount"])
        )


    ##updating changed facts

        if changed:

            with engine.begin() as conn:

                conn.execute(
                    text("""
                        UPDATE Factexpense
                        SET
                            DateID = :DateID,
                            employeeID = :employeeID,
                            DeptID = :DeptID,
                            ExpenseType = :ExpenseType,
                            Vendor = :Vendor,
                            PaymentMode = :PaymentMode,
                            Amount = :Amount
                        WHERE ExpenseID = :ExpenseID
                    """),
                    {
                        "DateID": row["DateID"],
                        "employeeID": row["employeeID"],
                        "DeptID": row["DeptID"],
                        "ExpenseType": row["ExpenseType"],
                        "Vendor": row["Vendor"],
                        "PaymentMode": row["PaymentMode"],
                        "Amount": row["Amount"],
                        "ExpenseID": expense_id
                    }
                )

            print(
                f"Updated ExpenseID {expense_id}"
            )

        ##if facts are unchanged

        else:

            print(
                f"Skipped ExpenseID {expense_id} - unchanged"
            )    

    logging.info("INCREMENTAL LOAD COMPLETED SUCCESSFULLY")

def full_load(engine):

    try:

        engine = get_engine()

        create_pk_fk_scd2(engine)

        df = clean_data()

        validate_full_load(df)

        dim_date, dim_department, dim_employee = create_dim_data(df) 

        load_data(dim_date, dim_department, dim_employee, engine)

        dim_department_db, dim_employee_db, dim_date_db = read_data(engine)

        fact_build_load(df, dim_department_db, dim_employee_db, dim_date_db, engine)
    
    except Exception as e:
        logging.exception("FULL LOAD FAILED")


def incremental_load(engine):

    try:

        engine = get2_engine()

        df = clean_data()

        validate_incremental_load(df)

        dim_employee_db = read_data2(engine)

        create_sd2_dim(df, engine, dim_employee_db)

        dim_department_db, dim_employee_db, dim_date_db = find_insert_new_dim(engine, df)

        fact_expense, existing_expense_ids, fact_db = reading_inserting_n_facts(engine, df, dim_department_db, dim_employee_db, dim_date_db )

        changing_existing_facts(fact_expense, existing_expense_ids, engine, fact_db)

    except Exception as e:
        logging.exception("INCREMENTAL LOAD FAILED")


def main():

    logging.info("ETL STARTED")

    engine = get2_engine()

    inspector = inspect(engine)

    if "factexpense" not in inspector.get_table_names():
        print("full load")
        full_load(engine)

    else:

        print("→ INCREMENTAL LOAD")
        incremental_load(engine)

if __name__ == "__main__":
    main()