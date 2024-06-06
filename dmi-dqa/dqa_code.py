import pandas as pd
from sqlalchemy import create_engine, text
from tabulate import tabulate

# Function to create a PostgreSQL connection string
def create_postgres_engine(ip_address, port, username, password, database):
    connection_string = f'postgresql://{username}:{password}@{ip_address}:{port}/{database}'
    return create_engine(connection_string)

# Parameters for source and target databases
source_db_params = {
    'ip_address': '<ip_address>',
    'port': '<port>',
    'username': '<username>',
    'password': '<password>',
    'database': '<database>'
}

target_db_params = {
    'ip_address': '<ip_address>',
    'port': '<port>',
    'username': '<username>',
    'password': '<password>',
    'database': '<database>'
}

# Create engine for source and target databases
source_engine = create_postgres_engine(**source_db_params)
target_engine = create_postgres_engine(**target_db_params)

# Test connection to source database
try:
    with source_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Source database connection successful.")
except Exception as e:
    print(f"Source database connection failed: {e}")

# Test connection to target database
try:
    with target_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Target database connection successful.")
except Exception as e:
    print(f"Target database connection failed: {e}")
    
# Row Count Check on Screening
source_row_count = pd.read_sql('SELECT COUNT(unique_id) FROM central_raw_mortality.mortality_surveillance where screeningdate is not NULL;', source_engine)
target_row_count = pd.read_sql('SELECT sum(screened) AS "SUM(screened)"FROM dmi_reporting.aggregate_mortality_survillence_report', target_engine)
if source_row_count.iloc[0,0] == target_row_count.iloc[0,0]:
    print(f"Screening_raw: {source_row_count}, Screening_reporting: {target_row_count}")
assert source_row_count.iloc[0,0] == target_row_count.iloc[0,0], "Row count mismatch on Screening"

# Row Count Check on ELIGIBLE 
source_row_count = pd.read_sql('SELECT COUNT(unique_id) FROM central_raw_mortality.mortality_surveillance where eligible = 1;', source_engine)
target_row_count = pd.read_sql('SELECT sum(eligible) AS "SUM(eligible)" FROM dmi_reporting.aggregate_mortality_survillence_report;', target_engine)
if source_row_count.iloc[0,0] == target_row_count.iloc[0,0]:
    print(f"eligible_raw: {source_row_count}, eligible_reporting: {target_row_count}")
assert source_row_count.iloc[0,0] == target_row_count.iloc[0,0], "Row count mismatch on Screening"

# # Schema Validation
source_schema = pd.read_sql("SELECT COUNT(unique_id)  FROM central_raw_mortality.mortality_surveillance where enrolled = '1';", source_engine)
target_schema = pd.read_sql("SELECT sum(enrolled)  FROM dmi_reporting.aggregate_mortality_survillence_report;", target_engine)
if source_row_count.iloc[0,0] == target_row_count.iloc[0,0]:
    print(f"enrolled_raw: {source_row_count}, enrolled_reporting: {target_row_count}")
    
# assert source_schema.equals(target_schema), "Schema mismatch"

# # Additional checks...

