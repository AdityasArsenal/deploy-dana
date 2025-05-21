import json
import os
import pyodbc
import datetime
import re
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor

# Database connection configuration
# Replace with actual values from your connection string
DB_CONFIG = {
    'server': 'esgdatadb.database.windows.net,1433',
    'database': 'ESGDataDB',
    'username': 'CloudSAfbce9c74',
    'password': 'InsideOut@123',  # Replace with actual password
    'driver': '{ODBC Driver 18 for SQL Server}'
}

def get_connection():
    """
    Create a connection to the Azure SQL database
    """
    try:
        conn_str = (
            f'DRIVER={DB_CONFIG["driver"]};'
            f'SERVER={DB_CONFIG["server"]};'
            f'DATABASE={DB_CONFIG["database"]};'
            f'UID={DB_CONFIG["username"]};'
            f'PWD={DB_CONFIG["password"]};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=no;'
            f'Connection Timeout=60;'
        )
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        # Try with ODBC Driver 17 as fallback
        try:
            alt_conn_str = (
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={DB_CONFIG["server"]};'
                f'DATABASE={DB_CONFIG["database"]};'
                f'UID={DB_CONFIG["username"]};'
                f'PWD={DB_CONFIG["password"]};'
                f'Encrypt=yes;'
                f'TrustServerCertificate=no;'
                f'Connection Timeout=60;'
            )
            return pyodbc.connect(alt_conn_str)
        except pyodbc.Error as e2:
            print(f"Error with fallback connection: {e2}")
            raise

def import_company(conn, company_name, company_data):
    """
    Import company information into the database
    
    Args:
        conn: Database connection
        company_name: Name of the company
        company_data: Company data from JSON
    
    Returns:
        Company ID
    """
    cursor = conn.cursor()
    
    # Extract company info
    company_info = company_data.get('company_info', {})
    company_identifier = company_info.get('company_identifier', '')
    identifier_scheme = company_info.get('identifier_scheme', '')
    
    # Get reporting period
    reporting_period = company_info.get('reporting_period', {})
    start_date = reporting_period.get('start_date', None)
    end_date = reporting_period.get('end_date', None)
    
    # Convert dates to proper format if they exist
    if start_date:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # Check if company already exists
    cursor.execute(
        'SELECT CompanyID FROM Companies WHERE CompanyIdentifier = ?',
        (company_identifier,)
    )
    row = cursor.fetchone()
    
    if row:
        company_id = row[0]
    else:
        # Insert new company
        cursor.execute(
            '''
            INSERT INTO Companies (CompanyName, CompanyIdentifier, IdentifierScheme, 
                                  ReportingPeriodStart, ReportingPeriodEnd)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (company_name, company_identifier, identifier_scheme, start_date, end_date)
        )
        conn.commit()
        
        # Get the ID of the newly inserted company
        cursor.execute(
            'SELECT CompanyID FROM Companies WHERE CompanyIdentifier = ?',
            (company_identifier,)
        )
        company_id = cursor.fetchone()[0]
    
    cursor.close()
    return company_id

def batch_import_units(conn, units_data, batch_size=100):
    """
    Import units into the database using batch processing
    
    Args:
        conn: Database connection
        units_data: Units data from JSON
        batch_size: Number of units to process in each batch
    
    Returns:
        Dictionary mapping unit references to unit IDs
    """
    cursor = conn.cursor()
    unit_id_map = {}
    
    # Get all existing units first to avoid redundant queries
    cursor.execute('SELECT UnitRef, UnitID FROM Units')
    for row in cursor.fetchall():
        unit_id_map[row[0]] = row[1]
    
    # Process units in batches
    unit_batch = []
    
    for unit_ref, unit_info in units_data.items():
        # Skip if already in our map
        if unit_ref in unit_id_map:
            continue
            
        # Extract unit info
        unit_type = unit_info.get('type', '')
        unit_value = unit_info.get('value', None)
        numerator = unit_info.get('numerator', None)
        denominator = unit_info.get('denominator', None)
        
        unit_batch.append((unit_ref, unit_type, unit_value, numerator, denominator))
        
        # Process batch if it reaches the batch size
        if len(unit_batch) >= batch_size:
            insert_unit_batch(conn, cursor, unit_batch, unit_id_map)
            unit_batch = []
    
    # Process any remaining units
    if unit_batch:
        insert_unit_batch(conn, cursor, unit_batch, unit_id_map)
    
    cursor.close()
    return unit_id_map

def insert_unit_batch(conn, cursor, unit_batch, unit_id_map):
    """
    Insert a batch of units and update the ID map
    """
    try:
        # Use a single transaction for the batch
        for unit_data in unit_batch:
            unit_ref, unit_type, unit_value, numerator, denominator = unit_data
            
            # Insert new unit
            cursor.execute(
                '''
                INSERT INTO Units (UnitRef, UnitType, UnitValue, Numerator, Denominator)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (unit_ref, unit_type, unit_value, numerator, denominator)
            )
            
            # Get the ID of the newly inserted unit
            cursor.execute(
                'SELECT UnitID FROM Units WHERE UnitRef = ?',
                (unit_ref,)
            )
            unit_id = cursor.fetchone()[0]
            unit_id_map[unit_ref] = unit_id
        
        # Commit the batch
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting unit batch: {e}")
        # Fall back to individual inserts
        for unit_data in unit_batch:
            try:
                unit_ref, unit_type, unit_value, numerator, denominator = unit_data
                cursor.execute(
                    '''
                    INSERT INTO Units (UnitRef, UnitType, UnitValue, Numerator, Denominator)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (unit_ref, unit_type, unit_value, numerator, denominator)
                )
                conn.commit()
                
                cursor.execute(
                    'SELECT UnitID FROM Units WHERE UnitRef = ?',
                    (unit_ref,)
                )
                unit_id = cursor.fetchone()[0]
                unit_id_map[unit_ref] = unit_id
            except Exception as e:
                conn.rollback()
                print(f"Error inserting unit {unit_ref}: {e}")

def batch_import_contexts(conn, contexts_data, batch_size=50):
    """
    Import contexts into the database using batch processing
    
    Args:
        conn: Database connection
        contexts_data: Contexts data from JSON
        batch_size: Number of contexts to process in each batch
    
    Returns:
        Dictionary mapping context references to context IDs
    """
    cursor = conn.cursor()
    context_id_map = {}
    
    # Get all existing contexts first to avoid redundant queries
    cursor.execute('SELECT ContextRef, ContextID FROM Contexts')
    for row in cursor.fetchall():
        context_id_map[row[0]] = row[1]
    
    # Process contexts in batches
    context_batch = []
    
    for context_ref, context_info in contexts_data.items():
        # Skip if already in our map
        if context_ref in context_id_map:
            continue
            
        # Extract entity info
        entity = context_info.get('entity', {})
        entity_identifier = entity.get('identifier', '')
        entity_scheme = entity.get('scheme', '')
        
        # Extract period info
        period = context_info.get('period', {})
        period_type = period.get('type', '')
        start_date = period.get('start_date', None)
        end_date = period.get('end_date', None)
        instant_date = period.get('instant', None)
        
        # Convert dates to proper format if they exist
        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        if instant_date:
            instant_date = datetime.datetime.strptime(instant_date, '%Y-%m-%d').date()
        
        # Convert scenario dimensions to JSON
        scenario = context_info.get('scenario', {})
        scenario_dimensions = json.dumps(scenario) if scenario else None
        
        context_batch.append((
            context_ref, entity_identifier, entity_scheme, period_type, 
            start_date, end_date, instant_date, scenario_dimensions
        ))
        
        # Process batch if it reaches the batch size
        if len(context_batch) >= batch_size:
            insert_context_batch(conn, cursor, context_batch, context_id_map)
            context_batch = []
    
    # Process any remaining contexts
    if context_batch:
        insert_context_batch(conn, cursor, context_batch, context_id_map)
    
    cursor.close()
    return context_id_map

def insert_context_batch(conn, cursor, context_batch, context_id_map):
    """
    Insert a batch of contexts and update the ID map
    """
    try:
        # Use a single transaction for the batch
        for context_data in context_batch:
            (context_ref, entity_identifier, entity_scheme, period_type, 
             start_date, end_date, instant_date, scenario_dimensions) = context_data
            
            # Insert new context
            cursor.execute(
                '''
                INSERT INTO Contexts (ContextRef, EntityIdentifier, EntityScheme, 
                                     PeriodType, PeriodStartDate, PeriodEndDate, 
                                     PeriodInstantDate, ScenarioDimensions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (context_ref, entity_identifier, entity_scheme, period_type, 
                 start_date, end_date, instant_date, scenario_dimensions)
            )
            
            # Get the ID of the newly inserted context
            cursor.execute(
                'SELECT ContextID FROM Contexts WHERE ContextRef = ?',
                (context_ref,)
            )
            context_id = cursor.fetchone()[0]
            context_id_map[context_ref] = context_id
        
        # Commit the batch
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting context batch: {e}")
        # Fall back to individual inserts
        for context_data in context_batch:
            try:
                (context_ref, entity_identifier, entity_scheme, period_type, 
                 start_date, end_date, instant_date, scenario_dimensions) = context_data
                cursor.execute(
                    '''
                    INSERT INTO Contexts (ContextRef, EntityIdentifier, EntityScheme, 
                                        PeriodType, PeriodStartDate, PeriodEndDate, 
                                        PeriodInstantDate, ScenarioDimensions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (context_ref, entity_identifier, entity_scheme, period_type, 
                    start_date, end_date, instant_date, scenario_dimensions)
                )
                conn.commit()
                
                cursor.execute(
                    'SELECT ContextID FROM Contexts WHERE ContextRef = ?',
                    (context_ref,)
                )
                context_id = cursor.fetchone()[0]
                context_id_map[context_ref] = context_id
            except Exception as e:
                conn.rollback()
                print(f"Error inserting context {context_ref}: {e}")

def ensure_kpi_definitions(conn, kpi_names):
    """
    Ensure KPI definitions exist in the database for a batch of KPI names
    
    Args:
        conn: Database connection
        kpi_names: List of KPI names
    
    Returns:
        Dictionary mapping KPI names to KPI IDs
    """
    cursor = conn.cursor()
    kpi_id_map = {}
    
    # Get all existing KPI definitions
    try:
        cursor.execute('SELECT KPIName, KPIID FROM KPIDefinitions')
        for row in cursor.fetchall():
            kpi_id_map[row[0]] = row[1]
    except Exception as e:
        print(f"Error fetching existing KPI definitions: {e}")
    
    # Get all categories
    category_map = {}
    try:
        cursor.execute('SELECT CategoryName, CategoryID FROM KPICategories')
        for row in cursor.fetchall():
            category_map[row[0]] = row[1]
    except Exception as e:
        print(f"Error fetching existing KPI categories: {e}")
    
    # Create missing categories if needed
    default_categories = ['Environmental', 'Social', 'Governance', 'Economic', 'Other']
    for category_name in default_categories:
        if category_name not in category_map:
            try:
                cursor.execute(
                    'INSERT INTO KPICategories (CategoryName, Description) VALUES (?, ?)',
                    (category_name, f'Auto-categorized {category_name} metrics')
                )
                conn.commit()
                
                cursor.execute(
                    'SELECT CategoryID FROM KPICategories WHERE CategoryName = ?',
                    (category_name,)
                )
                category_map[category_name] = cursor.fetchone()[0]
            except Exception as e:
                conn.rollback()
                print(f"Error creating category {category_name}: {e}")
    
    # Filter out KPI names that already exist in the database
    new_kpi_names = [name for name in kpi_names if name not in kpi_id_map]
    
    # Process new KPI names in smaller batches for better error handling
    batch_size = 50
    for i in range(0, len(new_kpi_names), batch_size):
        batch = new_kpi_names[i:i+batch_size]
        
        # Process each KPI in the batch
        for kpi_name in batch:
            # Try to identify category based on KPI name
            kpi_lower = kpi_name.lower()
            if re.search(r'emission|energy|water|waste|environment|carbon|climate', kpi_lower):
                category_name = 'Environmental'
            elif re.search(r'employee|worker|staff|gender|social|community|health|safety', kpi_lower):
                category_name = 'Social'
            elif re.search(r'governance|board|director|ethic|compliance|policy', kpi_lower):
                category_name = 'Governance'
            elif re.search(r'revenue|profit|economic|financial|turnover|income|expense', kpi_lower):
                category_name = 'Economic'
            else:
                category_name = 'Other'
                
            category_id = category_map.get(category_name)
            
            # Determine data type based on name patterns
            if re.search(r'count|number|amount|total', kpi_lower):
                data_type = 'numeric'
            elif re.search(r'date|when', kpi_lower):
                data_type = 'date'
            elif re.search(r'is|has|whether', kpi_lower):
                data_type = 'boolean'
            else:
                data_type = 'text'
            
            try:
                # Double check if KPI already exists to avoid unique constraint error
                cursor.execute(
                    'SELECT KPIID FROM KPIDefinitions WHERE KPIName = ?',
                    (kpi_name,)
                )
                existing = cursor.fetchone()
                if existing:
                    # KPI already exists, use existing ID
                    kpi_id_map[kpi_name] = existing[0]
                    continue
                    
                # Insert new KPI definition
                cursor.execute(
                    '''
                    INSERT INTO KPIDefinitions (KPIName, CategoryID, Description, DataType)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (kpi_name, category_id, f'Auto-generated definition for {kpi_name}', data_type)
                )
                conn.commit()
                
                # Get the ID of the newly inserted KPI definition
                cursor.execute(
                    'SELECT KPIID FROM KPIDefinitions WHERE KPIName = ?',
                    (kpi_name,)
                )
                kpi_id_map[kpi_name] = cursor.fetchone()[0]
            except Exception as e:
                conn.rollback()
                print(f"Error creating KPI definition for {kpi_name}: {e}")
                # Continue with other KPIs even if this one fails
    
    cursor.close()
    return kpi_id_map

def batch_import_kpi_facts(conn, company_id, kpis_data, unit_id_map, context_id_map, batch_size=200):
    """
    Import KPI facts into the database using batch processing
    
    Args:
        conn: Database connection
        company_id: Company ID
        kpis_data: KPIs data from JSON
        unit_id_map: Dictionary mapping unit references to unit IDs
        context_id_map: Dictionary mapping context references to context IDs
        batch_size: Number of facts to process in each batch
    """
    # Get all KPI names first and filter valid KPIs
    kpi_names = set()
    valid_kpis = []
    
    # Count total KPIs for progress reporting
    total_kpis = len(kpis_data)
    print(f"Processing {total_kpis} KPI facts...")
    
    # First pass - collect KPI names and validate KPIs
    skipped_kpis = 0
    for idx, kpi in enumerate(kpis_data):
        if idx % 1000 == 0 and idx > 0:
            print(f"Validating KPIs: {idx}/{total_kpis} processed")
            
        kpi_name = kpi.get('name', '')
        context_ref = kpi.get('context_ref', '')
        
        # Skip KPIs with invalid data
        if not kpi_name or not context_ref or context_ref not in context_id_map:
            skipped_kpis += 1
            continue
            
        kpi_names.add(kpi_name)
        valid_kpis.append(kpi)
    
    print(f"Found {len(kpi_names)} unique KPI names, skipped {skipped_kpis} invalid KPIs")
    
    # Ensure all KPI definitions exist in the database
    print(f"Ensuring KPI definitions exist in database...")
    kpi_id_map = ensure_kpi_definitions(conn, list(kpi_names))
    
    # Filter out KPIs that couldn't be defined
    filtered_kpis = []
    for kpi in valid_kpis:
        if kpi.get('name', '') in kpi_id_map:
            filtered_kpis.append(kpi)
    
    print(f"Preparing to import {len(filtered_kpis)} valid KPI facts")
    
    # Process KPI facts in batches
    cursor = conn.cursor()
    
    # Prepare batches for insertion
    fact_batch = []
    inserted_count = 0
    error_count = 0
    
    for idx, kpi in enumerate(filtered_kpis):
        if idx % 1000 == 0 and idx > 0:
            print(f"Processing KPI facts: {idx}/{len(filtered_kpis)}")
            
        try:
            # Get KPI details
            kpi_name = kpi.get('name', '')
            raw_value = kpi.get('raw_value', None)
            numeric_value = kpi.get('numeric_value', None)
            context_ref = kpi.get('context_ref', '')
            unit_ref = kpi.get('unit_ref', '')
            decimals = kpi.get('decimals', None)
            
            # Get context ID
            context_id = context_id_map.get(context_ref)
            
            # Get unit ID if available
            unit_id = unit_id_map.get(unit_ref) if unit_ref else None
            
            # Get KPI ID
            kpi_id = kpi_id_map.get(kpi_name)
            
            # Get time period information
            period_start = kpi.get('period_start', None)
            period_end = kpi.get('period_end', None)
            period_instant = kpi.get('period_instant', None)
            
            # Convert dates to proper format if they exist
            if period_start:
                period_start = datetime.datetime.strptime(period_start, '%Y-%m-%d').date()
                reporting_year = period_start.year
                # Determine reporting quarter
                month = period_start.month
                reporting_quarter = (month - 1) // 3 + 1
            elif period_instant:
                period_instant = datetime.datetime.strptime(period_instant, '%Y-%m-%d').date()
                reporting_year = period_instant.year
                # Determine reporting quarter
                month = period_instant.month
                reporting_quarter = (month - 1) // 3 + 1
            else:
                reporting_year = None
                reporting_quarter = None
            
            # Convert decimals to integer if possible
            if decimals and decimals != 'INF':
                try:
                    decimals = int(decimals)
                except ValueError:
                    decimals = None
            else:
                decimals = None
            
            # Add to batch
            fact_batch.append((
                company_id, kpi_id, context_id, unit_id, raw_value, numeric_value, 
                decimals, period_start, period_end, period_instant, 
                reporting_year, reporting_quarter
            ))
            
            # Process batch if it reaches the batch size
            if len(fact_batch) >= batch_size:
                success = insert_kpi_fact_batch(conn, cursor, fact_batch)
                if success:
                    inserted_count += len(fact_batch)
                else:
                    error_count += len(fact_batch)
                fact_batch = []
        
        except Exception as e:
            error_count += 1
            if error_count < 10:  # Show only first few errors to avoid spam
                print(f"Error preparing KPI fact: {e}")
    
    # Process any remaining facts
    if fact_batch:
        success = insert_kpi_fact_batch(conn, cursor, fact_batch)
        if success:
            inserted_count += len(fact_batch)
        else:
            error_count += len(fact_batch)
    
    cursor.close()
    print(f"KPI facts import completed: {inserted_count} inserted, {error_count} errors")

def insert_kpi_fact_batch(conn, cursor, fact_batch):
    """
    Insert a batch of KPI facts
    
    Returns:
        bool: True if successful, False if error occurred
    """
    try:
        # Use a single transaction for the batch
        for fact_data in fact_batch:
            cursor.execute(
                '''
                INSERT INTO KPIFacts (CompanyID, KPIID, ContextID, UnitID, RawValue, 
                                    NumericValue, Decimals, PeriodStart, PeriodEnd, 
                                    PeriodInstant, ReportingYear, ReportingQuarter)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                fact_data
            )
        
        # Commit the batch
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error inserting KPI fact batch: {e}")
        
        # Try individual inserts for each fact
        success_count = 0
        for fact_data in fact_batch:
            try:
                cursor.execute(
                    '''
                    INSERT INTO KPIFacts (CompanyID, KPIID, ContextID, UnitID, RawValue, 
                                        NumericValue, Decimals, PeriodStart, PeriodEnd, 
                                        PeriodInstant, ReportingYear, ReportingQuarter)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    fact_data
                )
                conn.commit()
                success_count += 1
            except Exception as e:
                conn.rollback()
                if success_count < 5:  # Limit error logging
                    print(f"Error inserting individual KPI fact: {e}")
        
        if success_count > 0:
            print(f"Fallback insertion: {success_count}/{len(fact_batch)} facts inserted individually")
        
        return success_count > 0

def process_single_file(json_file, json_dir):
    """
    Process a single JSON file and import its data into the database
    
    Args:
        json_file: Name of the JSON file
        json_dir: Directory containing the JSON file
    """
    # Connect to the database
    conn = get_connection()
    
    try:
        company_name = Path(json_file).stem.replace('_', ' ')
        print(f"Processing {json_file}...")
        
        # Load JSON data
        with open(os.path.join(json_dir, json_file), 'r', encoding='utf-8') as f:
            company_data = json.load(f)
        
        # Import company and get company ID
        company_id = import_company(conn, company_name, company_data)
        
        # Import units and get unit ID map
        unit_id_map = batch_import_units(conn, company_data.get('units', {}))
        
        # Import contexts and get context ID map
        context_id_map = batch_import_contexts(conn, company_data.get('contexts', {}))
        
        # Import KPI facts
        batch_import_kpi_facts(conn, company_id, company_data.get('kpis', []), 
                               unit_id_map, context_id_map)
        
        print(f"Successfully imported {json_file}")
    except Exception as e:
        print(f"Error processing {json_file}: {e}")
    finally:
        conn.close()

def import_json_to_db(json_dir, max_workers=2):
    """
    Import JSON data into the database
    
    Args:
        json_dir: Directory with parsed JSON files
        max_workers: Maximum number of concurrent database connections
    """
    print(f"Starting import from {json_dir}...")
    start_time = time.time()
    
    # Get all JSON files
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    print(f"Found {len(json_files)} files to import")
    
    # Process files in parallel for better performance
    # Use fewer workers to avoid overwhelming the database
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_file, json_file, json_dir) 
                  for json_file in json_files]
        
        # Process results as they complete
        for i, future in enumerate(futures):
            try:
                future.result()  # Wait for completion
                print(f"Progress: {i+1}/{len(json_files)} files processed")
            except Exception as e:
                print(f"Error in worker thread: {e}")
    
    end_time = time.time()
    print(f"Import completed in {end_time - start_time:.2f} seconds!")

if __name__ == "__main__":
    import_json_to_db("parsed_data") 