import random
import string
import psycopg2
import pyodbc
from pathlib import Path
from datetime import datetime, timedelta
# Attempt to import pyDatalog, but make its usage conditional
try:
    import pyDatalog
    PYDATALOG_AVAILABLE = True
except ImportError:
    PYDATALOG_AVAILABLE = False
    print("Warning: pyDatalog library not found. Skipping pyDatalog related steps.")

import subprocess
import os
import re # For robust GO splitting

# --- CONFIGURATION ---
POSTGRES_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "edi_staging",
    "user": "postgres",
    "password": "ClearToFly1"
}
SQLSERVER_CONN = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": "localhost", 
    "database": "edi_production",
    "uid": "sa",
    "pwd": "ClearToFly1"
}
PG_SCHEMA = Path("database_scripts/postgresql_create_edi_databases.sql")
SQL_SCHEMA = Path("database_scripts/sqlserver_create_results_database.sql")

# --- DATABASE CREATION HELPERS ---

def create_postgres_database_if_not_exists():
    """Creates the PostgreSQL database if it doesn't already exist."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONN["host"],
            port=POSTGRES_CONN["port"],
            dbname="postgres", 
            user=POSTGRES_CONN["user"],
            password=POSTGRES_CONN["password"]
        )
        conn.autocommit = True 
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (POSTGRES_CONN["dbname"],))
            if not cur.fetchone():
                print(f"Creating PostgreSQL database {POSTGRES_CONN['dbname']}...")
                cur.execute(f"CREATE DATABASE \"{POSTGRES_CONN['dbname']}\"") 
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
        conn_str_master = (
            f"DRIVER={SQLSERVER_CONN['driver']};"
            f"SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE=master;" 
            f"UID={SQLSERVER_CONN['uid']};"
            f"PWD={SQLSERVER_CONN['pwd']};"
            f"TrustServerCertificate=yes;" 
        )
        conn = pyodbc.connect(conn_str_master, autocommit=True) 
        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.databases WHERE name = ?", (SQLSERVER_CONN['database'],))
        if not cur.fetchone():
            print(f"Creating SQL Server database {SQLSERVER_CONN['database']}...")
            cur.execute(f"CREATE DATABASE [{SQLSERVER_CONN['database']}]") 
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
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date(start_date_obj, end_date_obj):
    if start_date_obj > end_date_obj:
        # If start_date is somehow after end_date due to calculation, just return end_date
        # This can happen if timedelta calculations result in a negative span.
        return end_date_obj
    delta = end_date_obj - start_date_obj
    if delta.days < 0 : # Should be caught by above, but safety.
        return end_date_obj
    random_days = random.randint(0, delta.days)
    return (start_date_obj + timedelta(days=random_days))

def generate_facility_framework_data(n_orgs=5, n_regions=10, n_facilities=50, n_financial_classes=10, n_payers=10, n_departments=20):
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

def generate_random_claims(facility_ids, financial_class_codes, n_claims=100000):
    """Generates random claim data with dates suitable for PostgreSQL partitions."""
    claims = []
    today = datetime.now().date()
    # Generate dates within a tighter window to align with PG partitions: CurrentDate - 3 months to CurrentDate + 3 months
    date_range_start = today - timedelta(days=90) 
    date_range_end = today + timedelta(days=90)   

    current_year = datetime.now().year
    dob_start_date = datetime(1950, 1, 1).date()
    # Ensure dob_end_date is valid (at least 18 years ago)
    if current_year - 18 < 1950:
        dob_end_date = dob_start_date
    else:
        dob_end_date = datetime(current_year - 18, 12, 31).date()

    for _ in range(n_claims):
        if not facility_ids or not financial_class_codes:
            print("Warning: facility_ids or financial_class_codes is empty in generate_random_claims. Skipping claim generation.")
            break
        
        fac_id = random.choice(facility_ids)
        fc_code = random.choice(financial_class_codes)
        
        start_dt = random_date(date_range_start, date_range_end)
        
        end_dt_offset_max = (date_range_end - start_dt).days if date_range_end > start_dt else 0
        end_dt_offset_max = min(end_dt_offset_max, 30) # Cap at 30 days for claim duration
        end_dt_offset = random.randint(0, end_dt_offset_max)
        end_dt = start_dt + timedelta(days=end_dt_offset)


        service_line_start_dt_max_offset = (end_dt - start_dt).days if end_dt >= start_dt else 0
        service_line_start_dt = start_dt + timedelta(days=random.randint(0, service_line_start_dt_max_offset))
        
        service_line_end_dt_max_offset = (end_dt - service_line_start_dt).days if end_dt >= service_line_start_dt else 0
        service_line_end_dt = service_line_start_dt + timedelta(days=random.randint(0, service_line_end_dt_max_offset))


        claim = {
            "claim_id": random_string(12),
            "facility_id": fac_id,
            "patient_account": random_string(10),
            "start_date": start_dt,
            "end_date": end_dt,
            "financial_class": fc_code,
            "dob": random_date(dob_start_date, dob_end_date),
            "service_line_start": service_line_start_dt,
            "service_line_end": service_line_end_dt,
            "rvu": round(random.uniform(0.5, 5.0), 2),
            "units": random.randint(1, 10)
        }
        claims.append(claim)
    return claims

# --- DATABASE LOADERS ---

def run_sqlserver_sql(sql_path):
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
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str_target_db, autocommit=False)
        cur = conn.cursor()
        statements = re.split(r'^\s*GO\s*$', sql_script_content, flags=re.IGNORECASE | re.MULTILINE)
        for stmt_batch in statements:
            clean_stmt_batch = stmt_batch.strip()
            if clean_stmt_batch:
                try:
                    cur.execute(clean_stmt_batch)
                except pyodbc.Error as e:
                    print(f"Error executing SQL Server statement: {e}")
                    print(f"Failing Statement Batch (first 1000 chars):\n---\n{clean_stmt_batch[:100000]}...\n---")
                    raise 
        conn.commit()
        print(f"SQL Server script {sql_path} executed successfully.")
    except FileNotFoundError:
        print(f"Error: SQL script file not found at {sql_path}")
        raise
    except pyodbc.Error as e:
        print(f"A general pyodbc error occurred while running SQL Server script {sql_path}: {e}")
        if conn: conn.rollback()
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        if conn: conn.rollback()
        raise
    finally:
        if conn: conn.close()

def run_postgres_schema_with_psql(sql_path):
    print(f"Running PostgreSQL schema with psql: {sql_path}")
    resolved_sql_path = sql_path.resolve()
    if not resolved_sql_path.exists():
        print(f"Error: PostgreSQL schema file not found at {resolved_sql_path}")
        raise FileNotFoundError(f"PostgreSQL schema file not found at {resolved_sql_path}")
    cmd = [
        "psql", f"--host={POSTGRES_CONN['host']}", f"--port={str(POSTGRES_CONN['port'])}",
        f"--username={POSTGRES_CONN['user']}", f"--dbname={POSTGRES_CONN['dbname']}",
        "-v", "ON_ERROR_STOP=1", "-f", str(resolved_sql_path)
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = POSTGRES_CONN["password"]
    try:
        process = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True, encoding='utf-8')
        print(f"PostgreSQL schema {sql_path} loaded successfully.")
        if process.stdout: print("psql stdout:\n", process.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running psql for {sql_path} (return code {e.returncode}): {e}")
        print("psql stdout:\n", e.stdout)
        print("psql stderr:\n", e.stderr)
        raise
    except FileNotFoundError:
        print("Error: psql command not found. Ensure PostgreSQL client tools are installed and in PATH.")
        raise

def insert_facility_framework_sqlserver(facility_details_list, organization_codes, region_codes, financial_class_codes_master, payer_codes_master, department_codes_master):
    print("Populating facility framework tables in SQL Server...")
    conn = None
    try:
        conn_str = (
            f"DRIVER={SQLSERVER_CONN['driver']};SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE={SQLSERVER_CONN['database']};UID={SQLSERVER_CONN['uid']};"
            f"PWD={SQLSERVER_CONN['pwd']};TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        cur = conn.cursor()

        print("Inserting Organizations...")
        for org_code_val in organization_codes:
            org_name_val = f"Organization {org_code_val}"
            try:
                cur.execute("IF NOT EXISTS (SELECT 1 FROM dbo.Organizations WHERE OrganizationCode = ?) INSERT INTO dbo.Organizations (OrganizationName, OrganizationCode) VALUES (?, ?)", 
                            org_code_val, org_name_val, org_code_val)
            except pyodbc.Error as e: print(f"Error inserting organization {org_code_val}: {e}")
        conn.commit()

        print("Inserting Regions...")
        for reg_code_val in region_codes:
            reg_name_val = f"Region {reg_code_val}"
            try:
                cur.execute("IF NOT EXISTS (SELECT 1 FROM dbo.Regions WHERE RegionCode = ?) INSERT INTO dbo.Regions (RegionName, RegionCode) VALUES (?, ?)", 
                            reg_code_val, reg_name_val, reg_code_val)
            except pyodbc.Error as e: print(f"Error inserting region {reg_code_val}: {e}")
        conn.commit()

        print("Inserting Standard Payers...")
        for payer_code_val in payer_codes_master:
            payer_name_val = f"Payer {payer_code_val}"
            payer_category_val = "General Category"
            try:
                cur.execute("IF NOT EXISTS (SELECT 1 FROM dbo.StandardPayers WHERE StandardPayerCode = ?) INSERT INTO dbo.StandardPayers (StandardPayerCode, StandardPayerName, PayerCategory) VALUES (?, ?, ?)", 
                            payer_code_val, payer_code_val, payer_name_val, payer_category_val)
            except pyodbc.Error as e: print(f"Error inserting standard payer {payer_code_val}: {e}")
        conn.commit()
        
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
                reg_db_id = reg_id_map.get(reg_code_fk)
                payer_db_id_for_fc = payer_id_map.get(payer_code_fk_for_fc)
                if not org_db_id:
                    print(f"Skipping facility {facility_id_val} due to missing Organization ID for code {org_code_fk}.")
                    continue
                facility_name_val, facility_internal_code = f"Facility {facility_id_val}", facility_id_val
                city_val, state_val, bed_size_val, emr_val, fac_type_val, crit_access_val = "Gen City", "XX", random.randint(50,500), "EMR X", "Non-Teaching", "N"
                
                cur.execute("""
                    IF NOT EXISTS (SELECT 1 FROM dbo.Facilities WHERE FacilityId = ?)
                    INSERT INTO dbo.Facilities (FacilityId, FacilityName, FacilityCode, OrganizationId, RegionId, City, StateCode, BedSize, EmrSystem, FacilityType, CriticalAccess)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, facility_id_val, 
                     facility_id_val, facility_name_val, facility_internal_code, org_db_id, reg_db_id, city_val, state_val, bed_size_val, emr_val, fac_type_val, crit_access_val)

                fc_desc_val = f"Financial Class {financial_class_id_val}"
                if payer_db_id_for_fc:
                    cur.execute("""
                        IF NOT EXISTS (SELECT 1 FROM dbo.FinancialClasses WHERE FinancialClassId = ?)
                        INSERT INTO dbo.FinancialClasses (FinancialClassId, FinancialClassDescription, StandardPayerId, FacilityId) VALUES (?, ?, ?, ?)
                        """, financial_class_id_val, # For IF NOT EXISTS
                             financial_class_id_val, fc_desc_val, payer_db_id_for_fc, facility_id_val) # For INSERT
                else:
                    print(f"Warning: Skipping FC {financial_class_id_val} for facility {facility_id_val} due to missing Payer ID for {payer_code_fk_for_fc}.")
                
                dept_desc_val, dept_type_val = f"Department {department_code_val}", "General"
                # Separated SELECT and INSERT for ClinicalDepartments
                cur.execute("SELECT 1 FROM dbo.ClinicalDepartments WHERE ClinicalDepartmentCode = ? AND FacilityId = ?", 
                            department_code_val, facility_id_val)
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO dbo.ClinicalDepartments (ClinicalDepartmentCode, DepartmentDescription, FacilityId, DepartmentType) 
                        VALUES (?, ?, ?, ?)
                        """, department_code_val, dept_desc_val, facility_id_val, dept_type_val)
                conn.commit()
            except pyodbc.Error as e:
                print(f"Error processing/inserting for facility {facility_id_val}: {e}")
                conn.rollback()
        print("Finished SQL Server facility framework population.")
    except pyodbc.Error as e:
        print(f"Pyodbc error in insert_facility_framework_sqlserver: {e}")
        if conn: conn.rollback()
    except Exception as e:
        print(f"Unexpected error in insert_facility_framework_sqlserver: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def insert_claims_postgres(claims_data_list):
    if not claims_data_list:
        print("No claims data to insert into PostgreSQL.")
        return
    print(f"Inserting {len(claims_data_list)} random claims into PostgreSQL staging.claims...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()
        for claim_item in claims_data_list:
            cur.execute("""
                INSERT INTO staging.claims (
                    claim_id, facility_id, patient_account_number, service_date, financial_class_id, 
                    patient_dob, total_charge_amount, processing_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (claim_id, service_date) DO NOTHING;
            """, (
                claim_item["claim_id"], claim_item["facility_id"], claim_item["patient_account"],
                claim_item["start_date"], claim_item["financial_class"], claim_item["dob"],
                round(random.uniform(100.0, 5000.0), 2), 'PENDING'
            ))
        conn.commit()
        print(f"{cur.rowcount if cur else 0} claims affected in PostgreSQL.")
    except psycopg2.Error as e:
        print(f"Error inserting claims into PostgreSQL: {e}")
        if conn: conn.rollback()
    except Exception as e:
        print(f"Unexpected error during PostgreSQL claim insertion: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

def generate_random_rules(facility_ids_list, financial_class_codes_list, n_rules=50):
    rules = []
    if not facility_ids_list and not financial_class_codes_list: return rules
    for _ in range(n_rules):
        choices = []
        if facility_ids_list: choices.append("facility")
        if financial_class_codes_list: choices.append("financial_class")
        if not choices: choices.append("units") # Fallback to units if other lists are empty
        
        rule_type = random.choice(choices)
        if rule_type == "facility": rules.append(f"valid_claim(CLAIM) <= claim(CLAIM, '{random.choice(facility_ids_list)}', _, _, _, _, _, _, _, _, _)")
        elif rule_type == "financial_class": rules.append(f"valid_claim(CLAIM) <= claim(CLAIM, _, _, _, _, '{random.choice(financial_class_codes_list)}', _, _, _, _, _)")
        else: rules.append(f"valid_claim(CLAIM) <= claim(CLAIM, _, _, _, _, _, _, _, _, _, {random.randint(1, 10)})")
    return rules

def test_pydatalog_rules(claims_data, generated_rules):
    if not PYDATALOG_AVAILABLE:
        print("pyDatalog module not imported. Skipping pyDatalog tests.")
        return
    if not claims_data or not generated_rules:
        print("Skipping pyDatalog test due to empty claims or rules.")
        return

    print("Attempting pyDatalog tests. Note: This section may require user adaptation based on their pyDatalog version and API.")
    try:
        # Check for modern pyDatalog.Logic() pattern
        if hasattr(pyDatalog, 'Logic'):
            logic = pyDatalog.Logic()
            print("  pyDatalog.Logic() instance created. Further interaction (add_clause, ask) would use this 'logic' object.")
            print("  Current script uses global pyDatalog calls which might not be compatible with this 'Logic' instance pattern.")
            print("  Skipping detailed pyDatalog interaction; user may need to adapt this section for their pyDatalog version.")
            return # Exit early as global calls below will likely fail or not use 'logic'

        # Fallback for older style if Logic() is not the primary interface
        # Try to clear context - this is highly version dependent
        cleared_context = False
        if hasattr(pyDatalog, 'clear'):
            pyDatalog.clear()
            cleared_context = True
            print("  Called pyDatalog.clear()")
        elif hasattr(pyDatalog, 'program') and callable(getattr(pyDatalog.program, 'clear', None)):
            pyDatalog.program().clear()
            cleared_context = True
            print("  Called pyDatalog.program().clear()")
        
        if not cleared_context:
            print("  No standard pyDatalog clear method found. Results may be cumulative if script is re-run.")

        sample_size = min(len(claims_data), 5) # Further reduced sample
        print(f"  Attempting to assert {sample_size} facts...")
        for c_item in random.sample(claims_data, sample_size):
            if hasattr(pyDatalog, 'assert_fact'):
                 pyDatalog.assert_fact('claim',
                                      c_item["claim_id"], c_item["facility_id"], c_item["patient_account"],
                                      str(c_item["start_date"]), str(c_item["end_date"]), c_item["financial_class"],
                                      str(c_item["dob"]), str(c_item["service_line_start"]), str(c_item["service_line_end"]),
                                      float(c_item["rvu"]), int(c_item["units"]))
            else:
                print("  pyDatalog.assert_fact not found. Cannot assert facts.")
                break 
        
        print("  Attempting to load rules...")
        for rule_str in random.sample(generated_rules, min(len(generated_rules), 5)): # Test a few rules
            if hasattr(pyDatalog, 'load'):
                pyDatalog.load(rule_str)
            else:
                print("  pyDatalog.load not found. Cannot load rules.")
                break
        
        print("  Attempting to ask query...")
        if hasattr(pyDatalog, 'ask'):
            result = pyDatalog.ask('valid_claim(CLAIM)')
            if result: print(f"  Datalog: {len(result.answers)} claims matched a rule from sample.")
            else: print("  Datalog: No claims matched any rule from sample.")
        else:
            print("  pyDatalog.ask not found. Cannot query.")

    except AttributeError as ae:
        print(f"  pyDatalog AttributeError: {ae}. The pyDatalog API used may be incompatible with your installed version.")
    except Exception as e:
        print(f"  An error occurred during pyDatalog testing: {e}")


def main():
    print("=== EDI Claims Processor Setup ===")
    try:
        print("\n--- Step 1: Ensuring Databases Exist ---")
        create_postgres_database_if_not_exists()
        create_sqlserver_database_if_not_exists()

        print("\n--- Step 2: Loading PostgreSQL Schema ---")
        if PG_SCHEMA.exists():
            run_postgres_schema_with_psql(PG_SCHEMA)
        else:
            print(f"PostgreSQL schema file {PG_SCHEMA} not found. CRITICAL: Setup cannot continue.")
            return

        print("\n--- Step 3: Loading SQL Server Schema ---")
        if SQL_SCHEMA.exists():
            run_sqlserver_sql(SQL_SCHEMA)
        else:
            print(f"SQL Server schema file {SQL_SCHEMA} not found. CRITICAL: Setup cannot continue.")
            return

        print("\n--- Step 4: Generating Facility Framework Data ---")
        org_codes, reg_codes, fac_ids_list, fc_codes_list_master, p_codes_list_master, dept_codes_list_master, fac_details = \
            generate_facility_framework_data(n_orgs=3, n_regions=2, n_facilities=10, n_financial_classes=5, n_payers=5, n_departments=5)

        print("\n--- Step 5: Populating SQL Server Facility Framework ---")
        insert_facility_framework_sqlserver(fac_details, org_codes, reg_codes, fc_codes_list_master, p_codes_list_master, dept_codes_list_master)

        print("\n--- Step 6: Generating and Inserting Claims into PostgreSQL Staging ---")
        claims_for_pg = generate_random_claims(fac_ids_list, fc_codes_list_master, n_claims=100000)
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
