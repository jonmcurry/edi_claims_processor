import random
import string
import psycopg2
import pyodbc
from pathlib import Path
from datetime import datetime, timedelta
import pyDatalog # pyDatalog is not typically used with SQL Server, consider removing if not needed for other parts
import subprocess
import os

# --- CONFIGURATION ---
POSTGRES_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "edi_staging",
    "user": "postgres",
    "password": "ClearToFly1" # Consider using environment variables for passwords
}
SQLSERVER_CONN = {
    "driver": "{ODBC Driver 17 for SQL Server}", # Ensure this driver is installed
    "server": "localhost", # Or your SQL Server instance name e.g., localhost\SQLEXPRESS
    "database": "edi_production",
    "uid": "sa",
    "pwd": "ClearToFly1" # Consider using environment variables for passwords
}
PG_SCHEMA = Path("database_scripts/postgresql_create_edi_databases.sql")
SQL_SCHEMA = Path("database_scripts/sqlserver_create_results_database.sql")

# --- DATABASE CREATION HELPERS ---

def create_postgres_database_if_not_exists():
    """Creates the PostgreSQL database if it doesn't already exist."""
    conn = None
    try:
        # Connect to the default 'postgres' database to check/create edi_staging
        conn = psycopg2.connect(
            host=POSTGRES_CONN["host"],
            port=POSTGRES_CONN["port"],
            dbname="postgres", # Default database to connect for administrative tasks
            user=POSTGRES_CONN["user"],
            password=POSTGRES_CONN["password"]
        )
        conn.autocommit = True # Required for CREATE DATABASE
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_CONN["dbname"],))
            if not cur.fetchone():
                print(f"Creating PostgreSQL database {POSTGRES_CONN['dbname']}...")
                cur.execute(f"CREATE DATABASE \"{POSTGRES_CONN['dbname']}\"") # Use SQL parameters or f-string carefully; added quotes for safety
                print(f"PostgreSQL database {POSTGRES_CONN['dbname']} created.")
            else:
                print(f"PostgreSQL database {POSTGRES_CONN['dbname']} already exists.")
    except psycopg2.Error as e:
        print(f"Error connecting to or creating PostgreSQL database: {e}")
    finally:
        if conn:
            conn.close()

def create_sqlserver_database_if_not_exists():
    """Creates the SQL Server database if it doesn't already exist."""
    conn = None
    try:
        # Connection string to connect to the 'master' database for administrative tasks
        conn_str_master = (
            f"DRIVER={SQLSERVER_CONN['driver']};"
            f"SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE=master;"  # Connect to master to check/create other databases
            f"UID={SQLSERVER_CONN['uid']};"
            f"PWD={SQLSERVER_CONN['pwd']};"
            f"TrustServerCertificate=yes;" # Added for potential local dev SSL issues
        )
        conn = pyodbc.connect(conn_str_master, autocommit=True) # Autocommit for CREATE DATABASE
        cur = conn.cursor()
        # Check if the target database exists
        cur.execute("SELECT name FROM sys.databases WHERE name = ?", (SQLSERVER_CONN['database'],))
        if not cur.fetchone():
            print(f"Creating SQL Server database {SQLSERVER_CONN['database']}...")
            cur.execute(f"CREATE DATABASE [{SQLSERVER_CONN['database']}]") # Use f-string carefully or ensure db name is safe; added brackets
            print(f"SQL Server database {SQLSERVER_CONN['database']} created.")
        else:
            print(f"SQL Server database {SQLSERVER_CONN['database']} already exists.")
    except pyodbc.Error as e:
        print(f"Error connecting to or creating SQL Server database: {e}")
    finally:
        if conn:
            conn.close()

# --- DATA GENERATION HELPERS ---

def random_string(length=8):
    """Generates a random string of fixed length."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date(start_year=1950, end_year=2025):
    """Generates a random date within a given year range."""
    # Ensure start_year is not after end_year
    if start_year > end_year:
        start_year = end_year
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    if start > end: # Should not happen with the check above, but as a safeguard
        return datetime.now().date()
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).date()

def generate_facility_framework_data(n_orgs=5, n_regions=10, n_facilities=50, n_financial_classes=10, n_payers=10, n_departments=20):
    """Generates sample data for the facility framework."""
    organization_codes = [f"ORG{str(i).zfill(3)}" for i in range(1, n_orgs + 1)]
    region_codes = [f"REG{str(i).zfill(3)}" for i in range(1, n_regions + 1)]
    facility_ids = [f"FAC{str(i).zfill(3)}" for i in range(1, n_facilities + 1)]
    financial_class_codes = [f"FC{str(i).zfill(3)}" for i in range(1, n_financial_classes + 1)]
    payer_codes = [f"PAYER{str(i).zfill(3)}" for i in range(1, n_payers + 1)]
    department_codes = [f"DEPT{str(i).zfill(3)}" for i in range(1, n_departments + 1)]

    facility_details_list = []
    for fac_id in facility_ids:
        facility_details_list.append({
            "facility_id": fac_id,
            "organization_code": random.choice(organization_codes),
            "region_code": random.choice(region_codes),
            "financial_class_code": random.choice(financial_class_codes),
            "payer_code": random.choice(payer_codes),
            "department_code": random.choice(department_codes)
        })
    return organization_codes, region_codes, facility_ids, financial_class_codes, payer_codes, department_codes, facility_details_list

def generate_random_claims(facility_ids, financial_class_codes, n_claims=100_000):
    """Generates a list of random claim data, with dates suitable for PostgreSQL partitions."""
    claims = []
    current_year = datetime.now().year
    # Generate dates mostly for the previous and current year to align with typical PG partition setup
    date_start_year = current_year - 1
    date_end_year = current_year

    for _ in range(n_claims):
        if not facility_ids or not financial_class_codes: # Guard against empty lists
            print("Warning: facility_ids or financial_class_codes is empty in generate_random_claims. Skipping claim generation.")
            break
        fac_id = random.choice(facility_ids)
        fc_code = random.choice(financial_class_codes)
        
        start_dt = random_date(date_start_year, date_end_year)
        end_dt = start_dt + timedelta(days=random.randint(0, 30))
        # Cap end_dt to ensure it's not too far in the future if date_end_year is current year
        if end_dt > datetime(date_end_year, 12, 31).date():
            end_dt = datetime(date_end_year, 12, 31).date()


        service_line_start_dt = start_dt + timedelta(days=random.randint(0, (end_dt - start_dt).days if end_dt > start_dt else 0))
        service_line_end_dt = service_line_start_dt + timedelta(days=random.randint(0, (end_dt - service_line_start_dt).days if end_dt > service_line_start_dt else 0))

        claim = {
            "claim_id": random_string(12),
            "facility_id": fac_id,
            "patient_account": random_string(10),
            "start_date": start_dt, # This will be ServiceDate
            "end_date": end_dt, # Informational, not directly in dbo.Claims
            "financial_class": fc_code, # This is FinancialClassId
            "dob": random_date(1950, current_year - 18), # Ensure patient is at least 18
            "service_line_start": service_line_start_dt,
            "service_line_end": service_line_end_dt,
            "rvu": round(random.uniform(0.5, 5.0), 2),
            "units": random.randint(1, 10)
        }
        claims.append(claim)
    return claims

# --- DATABASE LOADERS ---

def run_sqlserver_sql(sql_path):
    """Executes a SQL script against the SQL Server database."""
    print(f"Running SQL Server script: {sql_path}")
    conn = None
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_script_content = f.read()

        conn_str_target_db = (
            f"DRIVER={SQLSERVER_CONN['driver']};"
            f"SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE={SQLSERVER_CONN['database']};"
            f"UID={SQLSERVER_CONN['uid']};"
            f"PWD={SQLSERVER_CONN['pwd']};"
            f"TrustServerCertificate=yes;" # Added for potential local dev SSL issues
        )
        conn = pyodbc.connect(conn_str_target_db, autocommit=False)
        cur = conn.cursor()

        # Split script by "GO" (case-insensitive, on its own line or end of line)
        # Regex to split by "GO" possibly surrounded by whitespace, on its own line or at end.
        import re
        statements = re.split(r'^\s*GO\s*$', sql_script_content, flags=re.IGNORECASE | re.MULTILINE)

        for stmt_batch in statements:
            clean_stmt_batch = stmt_batch.strip()
            if clean_stmt_batch:
                try:
                    cur.execute(clean_stmt_batch)
                except pyodbc.Error as e:
                    print(f"Error executing SQL Server statement: {e}")
                    print(f"Failing Statement Batch (first 1000 chars):\n---\n{clean_stmt_batch[:1000]}...\n---")
                    raise # Stop setup if schema fails
        conn.commit()
        print(f"SQL Server script {sql_path} executed successfully.")
    except FileNotFoundError:
        print(f"Error: SQL script file not found at {sql_path}")
        raise
    except pyodbc.Error as e:
        print(f"A general pyodbc error occurred while running SQL Server script {sql_path}: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def run_postgres_schema_with_psql(sql_path):
    """Executes a PostgreSQL schema script using psql."""
    print(f"Running PostgreSQL schema with psql: {sql_path}")
    resolved_sql_path = sql_path.resolve()
    if not resolved_sql_path.exists():
        print(f"Error: PostgreSQL schema file not found at {resolved_sql_path}")
        raise FileNotFoundError(f"PostgreSQL schema file not found at {resolved_sql_path}")

    cmd = [
        "psql",
        f"--host={POSTGRES_CONN['host']}",
        f"--port={str(POSTGRES_CONN['port'])}",
        f"--username={POSTGRES_CONN['user']}",
        f"--dbname={POSTGRES_CONN['dbname']}",
        "-v", "ON_ERROR_STOP=1", # Stop script on error
        "-f", str(resolved_sql_path)
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = POSTGRES_CONN["password"]

    try:
        process = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True, encoding='utf-8')
        print(f"PostgreSQL schema {sql_path} loaded successfully.")
        if process.stdout:
            print("psql stdout:\n", process.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running psql for {sql_path} (return code {e.returncode}): {e}")
        print("psql stdout:\n", e.stdout)
        print("psql stderr:\n", e.stderr)
        raise
    except FileNotFoundError:
        print("Error: psql command not found. Ensure PostgreSQL client tools are installed and in PATH.")
        raise


def insert_facility_framework_sqlserver(facility_details_list, organization_codes, region_codes, financial_class_codes_master, payer_codes_master, department_codes_master):
    """Populates the facility framework tables in SQL Server."""
    print("Populating facility framework tables in SQL Server...")
    conn = None
    try:
        conn_str = (
            f"DRIVER={SQLSERVER_CONN['driver']};"
            f"SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE={SQLSERVER_CONN['database']};"
            f"UID={SQLSERVER_CONN['uid']};"
            f"PWD={SQLSERVER_CONN['pwd']};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        cur = conn.cursor()

        print("Inserting Organizations...")
        for org_code_val in organization_codes:
            org_name_val = f"Organization {org_code_val}"
            try:
                cur.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.Organizations WHERE OrganizationCode = ?)
                    INSERT INTO dbo.Organizations (OrganizationName, OrganizationCode) VALUES (?, ?)
                """, org_code_val, org_name_val, org_code_val)
            except pyodbc.Error as e:
                print(f"Error inserting organization {org_code_val}: {e}")
        conn.commit()

        print("Inserting Regions...")
        for reg_code_val in region_codes:
            reg_name_val = f"Region {reg_code_val}"
            try:
                cur.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.Regions WHERE RegionCode = ?)
                    INSERT INTO dbo.Regions (RegionName, RegionCode) VALUES (?, ?)
                """, reg_code_val, reg_name_val, reg_code_val)
            except pyodbc.Error as e:
                print(f"Error inserting region {reg_code_val}: {e}")
        conn.commit()

        print("Inserting Standard Payers...")
        for payer_code_val in payer_codes_master:
            payer_name_val = f"Payer {payer_code_val}"
            payer_category_val = "General Category"
            try:
                # Corrected: Parameter for IF NOT EXISTS check + 3 for INSERT = 4 total parameters
                cur.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.StandardPayers WHERE StandardPayerCode = ?)
                    INSERT INTO dbo.StandardPayers (StandardPayerCode, StandardPayerName, PayerCategory) VALUES (?, ?, ?)
                """, payer_code_val, payer_code_val, payer_name_val, payer_category_val)
            except pyodbc.Error as e:
                print(f"Error inserting standard payer {payer_code_val}: {e}")
        conn.commit()

        # Fetch IDs after master data insertion
        org_id_map = {code: cur.execute("SELECT OrganizationId FROM dbo.Organizations WHERE OrganizationCode = ?", code).fetchone()[0]
                      for code in organization_codes if cur.execute("SELECT OrganizationId FROM dbo.Organizations WHERE OrganizationCode = ?", code).fetchone()}
        reg_id_map = {code: cur.execute("SELECT RegionId FROM dbo.Regions WHERE RegionCode = ?", code).fetchone()[0]
                      for code in region_codes if cur.execute("SELECT RegionId FROM dbo.Regions WHERE RegionCode = ?", code).fetchone()}
        payer_id_map = {code: cur.execute("SELECT StandardPayerId FROM dbo.StandardPayers WHERE StandardPayerCode = ?", code).fetchone()[0]
                        for code in payer_codes_master if cur.execute("SELECT StandardPayerId FROM dbo.StandardPayers WHERE StandardPayerCode = ?", code).fetchone()}
        
        print("Processing and inserting Facilities, FinancialClasses, ClinicalDepartments...")
        for fac_detail in facility_details_list:
            facility_id_val = fac_detail["facility_id"]
            org_code_fk = fac_detail["organization_code"]
            reg_code_fk = fac_detail["region_code"]
            financial_class_id_val = fac_detail["financial_class_code"]
            payer_code_fk_for_fc = fac_detail["payer_code"]
            department_code_val = fac_detail["department_code"]

            try:
                org_db_id = org_id_map.get(org_code_fk)
                reg_db_id = reg_id_map.get(reg_code_fk) # Can be NULL if region not found, schema allows NULL for RegionId in Facilities
                payer_db_id_for_fc = payer_id_map.get(payer_code_fk_for_fc)

                if not org_db_id:
                    print(f"Skipping facility {facility_id_val} due to missing Organization ID for code {org_code_fk}.")
                    continue

                facility_name_val = f"Facility {facility_id_val}"
                facility_internal_code = facility_id_val
                city_val, state_val, bed_size_val, emr_val, fac_type_val, crit_access_val = "Generated City", "XX", random.randint(50,500), "EMR System X", "Non-Teaching", "N"

                cur.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.Facilities WHERE FacilityId = ?)
                    INSERT INTO dbo.Facilities (FacilityId, FacilityName, FacilityCode, OrganizationId, RegionId, City, StateCode, BedSize, EmrSystem, FacilityType, CriticalAccess)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, facility_id_val, facility_name_val, facility_internal_code, org_db_id, reg_db_id, city_val, state_val, bed_size_val, emr_val, fac_type_val, crit_access_val)

                fc_desc_val = f"Financial Class {financial_class_id_val}"
                if payer_db_id_for_fc:
                    cur.execute("""
                        IF NOT EXISTS (SELECT 1 FROM dbo.FinancialClasses WHERE FinancialClassId = ?)
                        INSERT INTO dbo.FinancialClasses (FinancialClassId, FinancialClassDescription, StandardPayerId, FacilityId)
                        VALUES (?, ?, ?, ?)
                    """, financial_class_id_val, fc_desc_val, payer_db_id_for_fc, facility_id_val)
                else:
                    print(f"Warning: Skipping FinancialClass {financial_class_id_val} for facility {facility_id_val} due to missing Payer ID for code {payer_code_fk_for_fc}.")

                dept_desc_val = f"Department {department_code_val}"
                dept_type_val = "General"
                cur.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.ClinicalDepartments WHERE ClinicalDepartmentCode = ? AND FacilityId = ?)
                    INSERT INTO dbo.ClinicalDepartments (ClinicalDepartmentCode, DepartmentDescription, FacilityId, DepartmentType)
                    VALUES (?, ?, ?, ?)
                """, department_code_val, dept_desc_val, facility_id_val, dept_type_val)
                conn.commit() # Commit after each facility and its related children
            except pyodbc.Error as e:
                print(f"Error processing/inserting for facility {facility_id_val}: {e}")
                conn.rollback() # Rollback this specific facility's transaction
        print("Finished populating facility framework tables in SQL Server.")
    except pyodbc.Error as e:
        print(f"A pyodbc error occurred in insert_facility_framework_sqlserver: {e}")
        if conn: conn.rollback()
    except Exception as e:
        print(f"An unexpected error in insert_facility_framework_sqlserver: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


def insert_claims_postgres(claims_data_list):
    """Inserts generated claims into the PostgreSQL staging.claims table."""
    if not claims_data_list:
        print("No claims data to insert into PostgreSQL.")
        return
    print(f"Inserting {len(claims_data_list)} random claims into PostgreSQL staging.claims...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()
        
        # NOTE: Assumes your PostgreSQL staging.claims table has (claim_id, service_date) as part of its primary key or a unique constraint for ON CONFLICT to work as intended.
        # The schema defines PRIMARY KEY (claim_id, service_date)
        for claim_item in claims_data_list:
            cur.execute("""
                INSERT INTO staging.claims (
                    claim_id, facility_id, patient_account_number, service_date, financial_class_id, 
                    patient_dob, total_charge_amount, processing_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (claim_id, service_date) DO NOTHING;
            """, (
                claim_item["claim_id"],
                claim_item["facility_id"],
                claim_item["patient_account"],
                claim_item["start_date"], # Mapped to service_date
                claim_item["financial_class"], # Mapped to financial_class_id
                claim_item["dob"],
                round(random.uniform(100.0, 5000.0), 2),
                'PENDING'
            ))
        conn.commit()
        print(f"{cur.rowcount if cur else 0} claims affected by PostgreSQL insertion (includes ignored conflicts).")
    except psycopg2.Error as e:
        print(f"Error inserting claims into PostgreSQL: {e}")
        if conn: conn.rollback()
    except Exception as e:
        print(f"An unexpected error during PostgreSQL claim insertion: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()


def generate_random_rules(facility_ids_list, financial_class_codes_list, n_rules=200):
    """Generates random pyDatalog rules."""
    rules = []
    if not facility_ids_list and not financial_class_codes_list: # Avoid errors if lists are empty
        return rules

    for _ in range(n_rules):
        rule_type_choices = []
        if facility_ids_list: rule_type_choices.append("facility")
        if financial_class_codes_list: rule_type_choices.append("financial_class")
        if not rule_type_choices: # only units rule left if others are empty
             rule_type_choices.append("units")
        
        rule_type = random.choice(rule_type_choices)

        if rule_type == "facility":
            fac = random.choice(facility_ids_list)
            rules.append(f"valid_claim(CLAIM) <= claim(CLAIM, '{fac}', _, _, _, _, _, _, _, _, _)")
        elif rule_type == "financial_class":
            fc = random.choice(financial_class_codes_list)
            rules.append(f"valid_claim(CLAIM) <= claim(CLAIM, _, _, _, _, '{fc}', _, _, _, _, _)")
        else: # units rule
            units = random.randint(1, 10)
            rules.append(f"valid_claim(CLAIM) <= claim(CLAIM, _, _, _, _, _, _, _, _, _, {units})")
    return rules

def test_pydatalog_rules(claims_data, generated_rules):
    """Tests pyDatalog rules with a sample of claims data."""
    if not claims_data or not generated_rules:
        print("Skipping pyDatalog test due to empty claims or rules.")
        return

    print("Testing pyDatalog rules with random claims...")
    try:
        # Attempt to clear the pyDatalog context
        # Newer pyDatalog versions might use pyDatalog.program().clear() or other mechanisms
        if hasattr(pyDatalog, 'program') and callable(getattr(pyDatalog, 'program', None)) and hasattr(pyDatalog.program(), 'clear'):
            pyDatalog.program().clear()
        elif hasattr(pyDatalog, 'Fact') and hasattr(pyDatalog.Fact, 'clear_facts'): # Older/alternative
             pyDatalog.Fact.clear_facts()
        else:
            # As a last resort for some very old versions or if specific clear isn't found,
            # re-initialize the engine. This is a bit heavy-handed.
            # Or, you might need to manually delete facts: del pyDatalog.Fact['claim'] if that's your main fact.
            # For now, we'll proceed hoping one of the above worked or is not strictly necessary for simple script run.
            print("Could not find a standard pyDatalog.clear() or program().clear() method. Proceeding without explicit clear.")
            # Re-initialize the default program (might work for some versions)
            pyDatalog.create_program()


    except Exception as e_clear:
        print(f"Warning: Could not clear pyDatalog context cleanly: {e_clear}")


    # Assert facts from the claims data.
    for c_item in random.sample(claims_data, min(len(claims_data), 100)): # Test with a sample
        try:
            pyDatalog.assert_fact('claim',
                                  c_item["claim_id"], c_item["facility_id"], c_item["patient_account"],
                                  str(c_item["start_date"]), str(c_item["end_date"]), c_item["financial_class"],
                                  str(c_item["dob"]), str(c_item["service_line_start"]), str(c_item["service_line_end"]),
                                  float(c_item["rvu"]), int(c_item["units"]))
        except Exception as e_assert:
            print(f"Error asserting Datalog fact for claim {c_item['claim_id']}: {e_assert}")

    # Load the generated rules
    for rule_str in generated_rules:
        try:
            pyDatalog.load(rule_str)
        except Exception as e_load:
            print(f"Error loading Datalog rule '{rule_str}': {e_load}")

    # Query for valid claims
    try:
        result = pyDatalog.ask('valid_claim(CLAIM)')
        if result:
            print(f"Number of claims matching at least one Datalog rule: {len(result.answers)}")
        else:
            print("No claims matched any Datalog rule based on the current query.")
    except Exception as e_ask:
        print(f"Error asking Datalog query 'valid_claim(CLAIM)': {e_ask}")


def main():
    """Main setup function."""
    print("=== EDI Claims Processor Setup ===")
    try:
        print("\n--- Step 1: Ensuring Databases Exist ---")
        create_postgres_database_if_not_exists()
        create_sqlserver_database_if_not_exists()

        print("\n--- Step 2: Loading PostgreSQL Schema ---")
        if PG_SCHEMA.exists():
            run_postgres_schema_with_psql(PG_SCHEMA)
        else:
            print(f"PostgreSQL schema file {PG_SCHEMA} not found. CRITICAL: Setup cannot continue without PG schema.")
            return # Stop if critical file is missing

        print("\n--- Step 3: Loading SQL Server Schema ---")
        if SQL_SCHEMA.exists():
            run_sqlserver_sql(SQL_SCHEMA)
        else:
            print(f"SQL Server schema file {SQL_SCHEMA} not found. CRITICAL: Setup cannot continue without SQL Server schema.")
            return # Stop if critical file is missing

        print("\n--- Step 4: Generating Facility Framework Data ---")
        org_codes, reg_codes, fac_ids_list, fc_codes_list_master, p_codes_list_master, dept_codes_list_master, fac_details = \
            generate_facility_framework_data(n_orgs=3, n_regions=2, n_facilities=10, n_financial_classes=5, n_payers=5, n_departments=5)

        print("\n--- Step 5: Populating SQL Server Facility Framework ---")
        insert_facility_framework_sqlserver(fac_details, org_codes, reg_codes, fc_codes_list_master, p_codes_list_master, dept_codes_list_master)

        print("\n--- Step 6: Generating and Inserting Claims into PostgreSQL Staging ---")
        # For claims, use the fac_ids_list (FacilityId PKs) and fc_codes_list_master (FinancialClassId PKs) generated for SQL Server
        claims_for_pg = generate_random_claims(fac_ids_list, fc_codes_list_master, n_claims=1000)
        insert_claims_postgres(claims_for_pg)

        print("\n--- Step 7: Generating pyDatalog Rules ---")
        datalog_rules = generate_random_rules(fac_ids_list, fc_codes_list_master, n_rules=50)

        print("\n--- Step 8: Testing pyDatalog Rules Engine ---")
        test_pydatalog_rules(claims_for_pg, datalog_rules)

        print("\n=== Setup Complete ===")

    except Exception as e:
        print(f"\nAN ERROR OCCURRED DURING SETUP: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
