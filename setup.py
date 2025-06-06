import random
import string
import psycopg2
import pyodbc
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
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
RVU_DATA_CSV = Path("data/rvu_data/rvu_table.csv")

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

def load_rvu_data_to_sql_server():
    """Reads RVU data from CSV and loads it into the SQL Server RvuData table."""
    if not RVU_DATA_CSV.exists():
        print(f"Warning: RVU data file not found at {RVU_DATA_CSV}. Skipping RVU data load.")
        return

    print(f"Loading RVU data from {RVU_DATA_CSV} into SQL Server...")
    conn = None
    try:
        conn_str = (
            f"DRIVER={SQLSERVER_CONN['driver']};SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE={SQLSERVER_CONN['database']};UID={SQLSERVER_CONN['uid']};"
            f"PWD={SQLSERVER_CONN['pwd']};TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        cur = conn.cursor()

        # Clear existing data to prevent duplicates on re-run
        cur.execute("DELETE FROM dbo.RvuData")
        print("Cleared existing data from dbo.RvuData.")

        df = pd.read_csv(RVU_DATA_CSV)

        insert_query = "INSERT INTO dbo.RvuData (CptCode, Description, RvuValue) VALUES (?, ?, ?)"

        data_to_insert = []
        for index, row in df.iterrows():
            # Ensure 'rvu_value' column exists and handle potential NaN
            rvu_val = row.get('rvu_value')
            if pd.isna(rvu_val):
                print(f"Warning: Skipping row for CPT {row.get('cpt_code')} due to missing rvu_value.")
                continue

            data_to_insert.append((
                str(row.get('cpt_code')),
                str(row.get('description')),
                float(rvu_val)
            ))

        if data_to_insert:
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            print(f"Successfully loaded {len(data_to_insert)} records into dbo.RvuData.")
        else:
            print("No valid RVU data found to load.")

    except pd.errors.EmptyDataError:
        print(f"Warning: RVU data file {RVU_DATA_CSV} is empty.")
    except Exception as e:
        print(f"Error loading RVU data to SQL Server: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# --- DATA GENERATION HELPERS ---

def random_string(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date(start_date_obj, end_date_obj):
    if start_date_obj > end_date_obj:
        return end_date_obj
    delta = end_date_obj - start_date_obj
    if delta.days < 0 :
        return end_date_obj
    random_days = random.randint(0, delta.days)
    return (start_date_obj + timedelta(days=random_days))

def generate_facility_framework_data(n_orgs=5, n_regions=10, n_facilities=50, n_financial_classes=10, n_payers=10, n_departments=20):
    organization_codes = [f"ORG{str(i).zfill(3)}" for i in range(1, n_orgs + 1)]
    region_codes = [f"REG{str(i).zfill(3)}" for i in range(1, n_regions + 1)]
    facility_ids = [f"FAC{str(i).zfill(3)}" for i in range(1, n_facilities + 1)]
    financial_class_codes = [f"FC{str(i).zfill(3)}" for i in range(1, n_financial_classes + 1)]
    payer_codes = [f"PAYER{str(i).zfill(3)}" for i in range(1, n_payers + 1)]
    department_master_ids = [i for i in range(1, n_departments + 1)]

    facility_details_list = []
    for fac_id in facility_ids:
        facility_details_list.append({
            "facility_id": fac_id,
            "organization_code": random.choice(organization_codes),
            "region_code": random.choice(region_codes),
            "financial_class_code": random.choice(financial_class_codes),
            "payer_code": random.choice(payer_codes),
            "department_code": f"DEPT{str(random.choice(department_master_ids)).zfill(3)}"
        })
    return organization_codes, region_codes, facility_ids, financial_class_codes, payer_codes, department_master_ids, facility_details_list


def generate_random_claims(facility_ids, financial_class_codes, department_ids, n_claims=100000): # Default changed
    """Generates random claim data with dates suitable for PostgreSQL partitions."""
    claims = []
    today = datetime.now().date()
    date_range_start = today - timedelta(days=90)
    date_range_end = today + timedelta(days=90)

    current_year = datetime.now().year
    dob_start_date = datetime(1950, 1, 1).date()
    if current_year - 18 < 1950:
        dob_end_date = dob_start_date
    else:
        dob_end_date = datetime(current_year - 18, 12, 31).date()

    for _ in range(n_claims):
        if not facility_ids or not financial_class_codes or not department_ids:
            print("Warning: facility_ids, financial_class_codes, or department_ids is empty in generate_random_claims.")
            break

        fac_id = random.choice(facility_ids)
        fc_code = random.choice(financial_class_codes)
        dept_id = random.choice(department_ids) if department_ids else None

        start_dt = random_date(date_range_start, date_range_end)

        end_dt_offset_max = (date_range_end - start_dt).days if date_range_end > start_dt else 0
        end_dt_offset_max = min(end_dt_offset_max, 30)
        end_dt_offset = random.randint(0, end_dt_offset_max)
        end_dt = start_dt + timedelta(days=end_dt_offset)

        service_line_start_dt_max_offset = (end_dt - start_dt).days if end_dt >= start_dt else 0
        service_line_start_dt = start_dt + timedelta(days=random.randint(0, service_line_start_dt_max_offset))

        service_line_end_dt_max_offset = (end_dt - service_line_start_dt).days if end_dt >= service_line_start_dt else 0
        service_line_end_dt = service_line_start_dt + timedelta(days=random.randint(0, service_line_end_dt_max_offset))

        patient_age_val = random.randint(18, 90)
        total_charge_amount_val = round(random.uniform(50.0, 10000.0), 2)

        claim = {
            "claim_id": random_string(12),
            "facility_id": fac_id,
            "department_id": dept_id,
            "financial_class": fc_code,
            "patient_id": random_string(10),
            "patient_age": patient_age_val,
            "patient_dob": random_date(dob_start_date, dob_end_date), # Key is "patient_dob"
            "patient_sex": random.choice(['M', 'F', 'U']),
            "patient_account": random_string(10),
            "provider_id": random_string(8),
            "provider_type": random.choice(["Individual", "Group", "Facility"]),
            "rendering_provider_npi": random_string(10) if random.choice([True, False]) else None,
            "place_of_service": str(random.randint(11, 99)),
            "start_date": start_dt,
            "end_date": end_dt,
            "total_charge_amount": total_charge_amount_val,
            "total_claim_charges": round(total_charge_amount_val * random.uniform(0.95, 1.05), 2),
            "payer_name": f"Payer_{random_string(5)}",
            "primary_insurance_id": random_string(15) if random.choice([True, False]) else None,
            "secondary_insurance_id": random_string(15) if random.choice([True, False, False]) else None,
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
                    print(f"Failing Statement Batch (first 1000 chars):\n---\n{clean_stmt_batch[:1000]}...\n---")
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
                        """, financial_class_id_val,
                             financial_class_id_val, fc_desc_val, payer_db_id_for_fc, facility_id_val)
                else:
                    print(f"Warning: Skipping FC {financial_class_id_val} for facility {facility_id_val} due to missing Payer ID for {payer_code_fk_for_fc}.")

                dept_desc_val, dept_type_val = f"Department {department_code_val}", "General"

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

        sql_insert = """
            INSERT INTO staging.claims (
                claim_id, facility_id, department_id, financial_class_id, 
                patient_id, patient_age, patient_dob, patient_sex, patient_account_number,
                provider_id, provider_type, rendering_provider_npi, place_of_service,
                service_date, total_charge_amount, total_claim_charges, 
                payer_name, primary_insurance_id, secondary_insurance_id,
                processing_status 
            ) VALUES (
                %(claim_id)s, %(facility_id)s, %(department_id)s, %(financial_class)s,
                %(patient_id)s, %(patient_age)s, %(patient_dob)s, %(patient_sex)s, %(patient_account)s,
                %(provider_id)s, %(provider_type)s, %(rendering_provider_npi)s, %(place_of_service)s,
                %(start_date)s, %(total_charge_amount)s, %(total_claim_charges)s,
                %(payer_name)s, %(primary_insurance_id)s, %(secondary_insurance_id)s,
                'PENDING'
            )
            ON CONFLICT (claim_id, service_date) DO NOTHING;
        """

        batch_size = 5000
        for i in range(0, len(claims_data_list), batch_size):
            batch = claims_data_list[i:i + batch_size]
            for claim_item in batch:
                 # Ensure all keys referenced in sql_insert exist in claim_item
                required_keys = [
                    "claim_id", "facility_id", "department_id", "financial_class", "patient_id",
                    "patient_age", "patient_dob", "patient_sex", "patient_account", "provider_id",
                    "provider_type", "rendering_provider_npi", "place_of_service", "start_date",
                    "total_charge_amount", "total_claim_charges", "payer_name",
                    "primary_insurance_id", "secondary_insurance_id"
                ]
                for key in required_keys:
                    if key not in claim_item:
                        # Provide a default or handle missing key
                        # For example, for nullable fields, you might set to None
                        # For now, printing a warning and skipping if a critical key is missing.
                        # This shouldn't happen if generate_random_claims is correct.
                        if key == "patient_dob": # Specifically handling the previous 'dob' error source
                             claim_item[key] = claim_item.get("dob") # try to get it if it was old key
                        if claim_item.get(key) is None: # If it's still None, assign a default or skip
                            print(f"Warning: Missing key '{key}' in claim_item for claim_id {claim_item.get('claim_id', 'N/A')}. Setting to None or default.")
                            if key in ["rendering_provider_npi", "primary_insurance_id", "secondary_insurance_id", "department_id"]:
                                claim_item[key] = None # These can be NULL
                            else: # For other potentially NOT NULL fields, this might still cause DB errors
                                claim_item[key] = "MISSING_DATA" # Placeholder to avoid immediate KeyError

                cur.execute(sql_insert, claim_item)
            conn.commit()
            print(f"Committed batch of {len(batch)} claims to PostgreSQL.")

        print(f"Finished inserting claims into PostgreSQL.")
    except psycopg2.Error as e:
        print(f"Error inserting claims into PostgreSQL: {e}")
        if conn: conn.rollback()
    except KeyError as ke: # Catch KeyError specifically
        print(f"KeyError during PostgreSQL claim insertion: {ke}. Check claim_item dictionary keys.")
        if conn: conn.rollback()
    except Exception as e:
        print(f"An unexpected error during PostgreSQL claim insertion: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

def query_sample_claims_postgres(limit=10):
    """Queries and prints a sample of claims from PostgreSQL staging.claims."""
    print(f"\n--- Querying Sample Claims from PostgreSQL (Top {limit}) ---")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        query = f"""
            SELECT 
                claim_id,
                facility_id,
                department_id, 
                patient_id, 
                patient_age, 
                patient_sex, 
                provider_id, 
                provider_type, 
                rendering_provider_npi, 
                place_of_service, 
                service_date,
                total_charge_amount,
                total_claim_charges, 
                payer_name, 
                primary_insurance_id, 
                secondary_insurance_id,
                financial_class_id,
                processing_status
            FROM staging.claims
            ORDER BY created_date DESC
            LIMIT {limit};
        """
        cur.execute(query)
        rows = cur.fetchall()

        if not rows:
            print("No claims found in staging.claims.")
            return

        colnames = [desc[0] for desc in cur.description]
        print(" | ".join(colnames))
        print("-" * (sum(len(cn) for cn in colnames) + (len(colnames) - 1) * 3))
        for row in rows:
            print(" | ".join(str(value) if value is not None else "NULL" for value in row))

    except psycopg2.Error as e:
        print(f"Error querying claims from PostgreSQL: {e}")
    except Exception as e:
        print(f"An unexpected error during PostgreSQL sample query: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()


def generate_random_rules(facility_ids_list, financial_class_codes_list, n_rules=50):
    """Generates a list of dictionaries, each containing a datalog rule name and definition."""
    rules = []
    if not PYDATALOG_AVAILABLE: return rules
    if not facility_ids_list and not financial_class_codes_list: return rules
    
    generated_names = set()

    for _ in range(n_rules):
        choices = []
        if facility_ids_list: choices.append("facility")
        if financial_class_codes_list: choices.append("financial_class")
        if not choices: choices.append("units") 
        
        rule_type = random.choice(choices)
        rule_name = ""
        rule_def = ""

        if rule_type == "facility":
            facility = random.choice(facility_ids_list)
            rule_name = f"DLG_VALID_FACILITY_{facility}"
            if rule_name not in generated_names:
                rule_def = f"valid_claim(CLAIM) <= claim(CLAIM, '{facility}', _, _, _, _, _, _, _, _, _)"
        elif rule_type == "financial_class":
            fin_class = random.choice(financial_class_codes_list)
            rule_name = f"DLG_VALID_FINCLASS_{fin_class}"
            if rule_name not in generated_names:
                rule_def = f"valid_claim(CLAIM) <= claim(CLAIM, _, _, _, _, '{fin_class}', _, _, _, _, _)"
        else: # units rule
            units = random.randint(1, 10)
            rule_name = f"DLG_VALID_UNITS_GTE_{units}"
            if rule_name not in generated_names:
                 rule_def = f"valid_claim(CLAIM) <= claim(CLAIM, _, _, _, _, _, _, _, _, _, Units), Units >= {units}"
        
        if rule_name and rule_def:
            rules.append({"name": rule_name, "definition": rule_def})
            generated_names.add(rule_name)
            
    return rules

def insert_datalog_rules_to_db(rules_list):
    """Inserts generated Datalog rules into the edi.filters table."""
    if not rules_list:
        print("No datalog rules generated to insert.")
        return

    print(f"Attempting to insert {len(rules_list)} Datalog rules into PostgreSQL edi.filters table...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO edi.filters (
                filter_name, version, rule_definition, rule_type, is_active, is_latest_version, created_by
            ) VALUES (
                %(name)s, %(version)s, %(definition)s, 'DATALOG', TRUE, TRUE, 'setup.py'
            )
            ON CONFLICT (filter_name, version) DO NOTHING;
        """
        
        inserted_count = 0
        for rule_dict in rules_list:
            data = {
                "name": rule_dict['name'],
                "version": 1, # Defaulting version to 1 for this setup script
                "definition": rule_dict['definition']
            }
            cur.execute(insert_query, data)
            if cur.rowcount > 0:
                inserted_count += 1
        
        conn.commit()
        print(f"Successfully inserted {inserted_count} new Datalog rules. {len(rules_list) - inserted_count} rules already existed.")

    except psycopg2.Error as e:
        print(f"Error inserting Datalog rules into PostgreSQL: {e}")
        if conn: conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during Datalog rule insertion: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

def test_pydatalog_rules(claims_data, generated_rules):
    if not PYDATALOG_AVAILABLE:
        print("pyDatalog module not imported or available. Skipping pyDatalog tests.")
        return
    if not claims_data or not generated_rules:
        print("Skipping pyDatalog test due to empty claims or rules.")
        return

    print("Attempting pyDatalog tests. Note: This section may require user adaptation based on their pyDatalog version and API.")
    try:
        if hasattr(pyDatalog, 'Logic'):
            logic = pyDatalog.Logic()
            # Ensure terms used in rules and facts are declared for this Logic instance
            # pyDatalog.create_terms('claim, valid_claim, CLAIM') # This would be global, not instance specific usually.
            # For Logic instances, terms are often implicitly created or using logic. टर्म्स(...)
            print("  pyDatalog.Logic() instance created. Interaction should use this 'logic' object (e.g., logic.add_clause, logic.ask).")
            print("  The current script's global pyDatalog calls are likely incompatible.")
            print("  Skipping detailed pyDatalog interaction; user adaptation needed for their pyDatalog version.")
            return

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

        sample_size = min(len(claims_data), 5)
        print(f"  Attempting to assert {sample_size} facts using global pyDatalog.assert_fact...")
        if hasattr(pyDatalog, 'assert_fact'):
            for c_item in random.sample(claims_data, sample_size):
                pyDatalog.assert_fact('claim',
                                      c_item["claim_id"], c_item["facility_id"], c_item["patient_account"],
                                      str(c_item["start_date"]), str(c_item["end_date"]), c_item["financial_class"],
                                      str(c_item["patient_dob"]),
                                      str(c_item.get("service_line_start", "")), str(c_item.get("service_line_end", "")),
                                      float(c_item["rvu"]), int(c_item["units"]))
        else:
            print("  pyDatalog.assert_fact not found.")

        print("  Attempting to load rules using global pyDatalog.load...")
        if hasattr(pyDatalog, 'load'):
            # The test function now receives a list of rule definitions, not dictionaries
            for rule_str in random.sample(generated_rules, min(len(generated_rules), 5)):
                pyDatalog.load(rule_str)
        else:
            print("  pyDatalog.load not found.")

        print("  Attempting to ask query using global pyDatalog.ask...")
        if hasattr(pyDatalog, 'ask'):
            result = pyDatalog.ask('valid_claim(CLAIM)')
            if result: print(f"  Datalog: {len(result.answers)} claims matched a rule from sample.")
            else: print("  Datalog: No claims matched any rule from sample.")
        else:
            print("  pyDatalog.ask not found.")

    except AttributeError as ae:
        print(f"  pyDatalog AttributeError: {ae}. Your pyDatalog version may require using a pyDatalog.Logic() object and its methods.")
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

        print("\n--- Step 3.5: Loading RVU Data into SQL Server ---")
        load_rvu_data_to_sql_server()

        print("\n--- Step 4: Generating Facility Framework Data ---")
        org_codes, reg_codes, fac_ids_list, fc_codes_list_master, p_codes_list_master, dept_ids_master, fac_details = \
            generate_facility_framework_data(n_orgs=3, n_regions=2, n_facilities=10, n_financial_classes=5, n_payers=5, n_departments=5)

        print("\n--- Step 5: Populating SQL Server Facility Framework ---")
        insert_facility_framework_sqlserver(fac_details, org_codes, reg_codes, fc_codes_list_master, p_codes_list_master, dept_ids_master)

        print("\n--- Step 6: Generating and Inserting Claims into PostgreSQL Staging ---")
        claims_for_pg = generate_random_claims(fac_ids_list, fc_codes_list_master, dept_ids_master, n_claims=100000) # Changed to 100,000
        insert_claims_postgres(claims_for_pg)

        query_sample_claims_postgres(limit=5)

        print("\n--- Step 7: Generating pyDatalog Rules ---")
        datalog_rules = generate_random_rules(fac_ids_list, fc_codes_list_master, n_rules=50)

        print("\n--- Step 7.5: Storing pyDatalog Rules in DB ---")
        insert_datalog_rules_to_db(datalog_rules)

        print("\n--- Step 8: Testing pyDatalog Rules Engine ---")
        # Pass just the rule definitions to the test function
        rule_definitions_for_test = [r['definition'] for r in datalog_rules]
        test_pydatalog_rules(claims_for_pg, rule_definitions_for_test)

        print("\n=== Setup Complete ===")

    except Exception as e:
        print(f"\nAN ERROR OCCURRED DURING SETUP: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()