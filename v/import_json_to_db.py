import json
import os
import pyodbc
import datetime
import re
from pathlib import Path

# Database connection configuration
# Replace with actual values from your connection string
DB_CONFIG = {
    'server': 'esgdatadb.database.windows.net,1433',
    'database': 'ESGDataDB',
    'username': '',
    'password': 'YOUR_PASSWORD_HERE',  # Replace with actual password
    'driver': '{ODBC Driver 17 for SQL Server}'
}

def get_connection():
    """
    Create a connection to the Azure SQL database
    """
    conn_str = (
        f'DRIVER={DB_CONFIG["driver"]};'
        f'SERVER={DB_CONFIG["server"]};'
        f'DATABASE={DB_CONFIG["database"]};'
        f'UID={DB_CONFIG["username"]};'
        f'PWD={DB_CONFIG["password"]};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'Connection Timeout=30;'
    )
    return pyodbc.connect(conn_str)

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

def import_units(conn, units_data):
    """
    Import units into the database
    
    Args:
        conn: Database connection
        units_data: Units data from JSON
    
    Returns:
        Dictionary mapping unit references to unit IDs
    """
    cursor = conn.cursor()
    unit_id_map = {}
    
    for unit_ref, unit_info in units_data.items():
        # Extract unit info
        unit_type = unit_info.get('type', '')
        unit_value = unit_info.get('value', None)
        numerator = unit_info.get('numerator', None)
        denominator = unit_info.get('denominator', None)
        
        # Check if unit already exists
        cursor.execute(
            'SELECT UnitID FROM Units WHERE UnitRef = ?',
            (unit_ref,)
        )
        row = cursor.fetchone()
        
        if row:
            unit_id = row[0]
        else:
            # Insert new unit
            cursor.execute(
                '''
                INSERT INTO Units (UnitRef, UnitType, UnitValue, Numerator, Denominator)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (unit_ref, unit_type, unit_value, numerator, denominator)
            )
            conn.commit()
            
            # Get the ID of the newly inserted unit
            cursor.execute(
                'SELECT UnitID FROM Units WHERE UnitRef = ?',
                (unit_ref,)
            )
            unit_id = cursor.fetchone()[0]
        
        unit_id_map[unit_ref] = unit_id
    
    cursor.close()
    return unit_id_map

def import_contexts(conn, contexts_data):
    """
    Import contexts into the database
    
    Args:
        conn: Database connection
        contexts_data: Contexts data from JSON
    
    Returns:
        Dictionary mapping context references to context IDs
    """
    cursor = conn.cursor()
    context_id_map = {}
    
    for context_ref, context_info in contexts_data.items():
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
        
        # Check if context already exists
        cursor.execute(
            'SELECT ContextID FROM Contexts WHERE ContextRef = ?',
            (context_ref,)
        )
        row = cursor.fetchone()
        
        if row:
            context_id = row[0]
        else:
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
            conn.commit()
            
            # Get the ID of the newly inserted context
            cursor.execute(
                'SELECT ContextID FROM Contexts WHERE ContextRef = ?',
                (context_ref,)
            )
            context_id = cursor.fetchone()[0]
        
        context_id_map[context_ref] = context_id
    
    cursor.close()
    return context_id_map

def ensure_kpi_definition(conn, kpi_name):
    """
    Ensure a KPI definition exists in the database
    
    Args:
        conn: Database connection
        kpi_name: Name of the KPI
    
    Returns:
        KPI ID
    """
    cursor = conn.cursor()
    
    # Try to identify category based on KPI name
    category_id = None
    
    # Simplified category detection based on KPI name patterns
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
    
    # Get category ID
    cursor.execute(
        'SELECT CategoryID FROM KPICategories WHERE CategoryName = ?',
        (category_name,)
    )
    row = cursor.fetchone()
    if row:
        category_id = row[0]
    else:
        # Insert the category if it doesn't exist
        cursor.execute(
            'INSERT INTO KPICategories (CategoryName, Description) VALUES (?, ?)',
            (category_name, f'Auto-categorized {category_name} metrics')
        )
        conn.commit()
        
        cursor.execute(
            'SELECT CategoryID FROM KPICategories WHERE CategoryName = ?',
            (category_name,)
        )
        category_id = cursor.fetchone()[0]
    
    # Check if KPI already exists
    cursor.execute(
        'SELECT KPIID FROM KPIDefinitions WHERE KPIName = ?',
        (kpi_name,)
    )
    row = cursor.fetchone()
    
    if row:
        kpi_id = row[0]
    else:
        # Determine data type based on name patterns
        if re.search(r'count|number|amount|total', kpi_lower):
            data_type = 'numeric'
        elif re.search(r'date|when', kpi_lower):
            data_type = 'date'
        elif re.search(r'is|has|whether', kpi_lower):
            data_type = 'boolean'
        else:
            data_type = 'text'
        
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
        kpi_id = cursor.fetchone()[0]
    
    cursor.close()
    return kpi_id

def import_kpi_facts(conn, company_id, kpis_data, unit_id_map, context_id_map):
    """
    Import KPI facts into the database
    
    Args:
        conn: Database connection
        company_id: Company ID
        kpis_data: KPIs data from JSON
        unit_id_map: Dictionary mapping unit references to unit IDs
        context_id_map: Dictionary mapping context references to context IDs
    """
    cursor = conn.cursor()
    
    # Process each KPI
    for kpi in kpis_data:
        # Get KPI details
        kpi_name = kpi.get('name', '')
        raw_value = kpi.get('raw_value', None)
        numeric_value = kpi.get('numeric_value', None)
        context_ref = kpi.get('context_ref', '')
        unit_ref = kpi.get('unit_ref', '')
        decimals = kpi.get('decimals', None)
        
        # Skip if no context reference
        if not context_ref or context_ref not in context_id_map:
            continue
        
        # Get context ID
        context_id = context_id_map.get(context_ref)
        
        # Get unit ID if available
        unit_id = unit_id_map.get(unit_ref) if unit_ref else None
        
        # Get KPI ID, creating definition if needed
        kpi_id = ensure_kpi_definition(conn, kpi_name)
        
        # Get time period information
        period_start = kpi.get('period_start', None)
        period_end = kpi.get('period_end', None)
        period_instant = kpi.get('period_instant', None)
        
        # Convert dates to proper format if they exist
        if period_start:
            period_start = datetime.datetime.strptime(period_start, '%Y-%m-%d').date()
            reporting_year = period_start.year
        elif period_instant:
            period_instant = datetime.datetime.strptime(period_instant, '%Y-%m-%d').date()
            reporting_year = period_instant.year
        else:
            reporting_year = None
        
        # Determine reporting quarter (simple approach)
        reporting_quarter = None
        if period_start:
            month = datetime.datetime.strptime(period_start, '%Y-%m-%d').month
            reporting_quarter = (month - 1) // 3 + 1
        
        # Convert decimals to integer if possible
        if decimals and decimals != 'INF':
            try:
                decimals = int(decimals)
            except ValueError:
                decimals = None
        else:
            decimals = None
        
        # Insert KPI fact
        cursor.execute(
            '''
            INSERT INTO KPIFacts (CompanyID, KPIID, ContextID, UnitID, RawValue, 
                                 NumericValue, Decimals, PeriodStart, PeriodEnd, 
                                 PeriodInstant, ReportingYear, ReportingQuarter)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (company_id, kpi_id, context_id, unit_id, raw_value, numeric_value, 
             decimals, period_start, period_end, period_instant, 
             reporting_year, reporting_quarter)
        )
    
    conn.commit()
    cursor.close()

def import_json_to_db(json_dir):
    """
    Import JSON data into the database
    
    Args:
        json_dir: Directory with parsed JSON files
    """
    # Connect to the database
    conn = get_connection()
    
    try:
        # Get all JSON files
        json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
        
        for json_file in json_files:
            print(f"Processing {json_file}...")
            company_name = Path(json_file).stem.replace('_', ' ')
            
            # Load JSON data
            with open(os.path.join(json_dir, json_file), 'r', encoding='utf-8') as f:
                company_data = json.load(f)
            
            # Import company and get company ID
            company_id = import_company(conn, company_name, company_data)
            
            # Import units and get unit ID map
            unit_id_map = import_units(conn, company_data.get('units', {}))
            
            # Import contexts and get context ID map
            context_id_map = import_contexts(conn, company_data.get('contexts', {}))
            
            # Import KPI facts
            import_kpi_facts(conn, company_id, company_data.get('kpis', []), 
                             unit_id_map, context_id_map)
            
            print(f"Successfully imported {json_file}")
        
        print("Import completed successfully!")
    
    finally:
        conn.close()

if __name__ == "__main__":
    import_json_to_db("parsed_data") 