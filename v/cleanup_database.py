import pyodbc
import time

# Database connection configuration
# Replace with your actual values
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

def cleanup_database():
    """
    Clean up all data from the database tables in the correct order
    to respect foreign key constraints
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("Starting database cleanup...")
        
        # Tables to clean up in order (child tables first, then parent tables)
        tables = [
            "KPIFacts",
            "KPIDefinitions",
            "KPICategories",
            "Contexts",
            "Units",
            "Companies"
        ]
        
        # Delete data from each table
        for table in tables:
            try:
                print(f"Deleting data from {table}...")
                
                # Delete all data from the table
                cursor.execute(f"DELETE FROM {table}")
                
                # If the table has an identity column, reset it
                try:
                    cursor.execute(f"DBCC CHECKIDENT ('{table}', RESEED, 0)")
                except pyodbc.Error as e:
                    # If CHECKIDENT fails, it might be because there's no identity column
                    # or Azure SQL DB might have a different way to reset identity
                    print(f"Note: Could not reset identity column for {table}: {e}")
                
                conn.commit()
                print(f"Successfully deleted data from {table}")
            except Exception as e:
                conn.rollback()
                print(f"Error deleting data from {table}: {e}")
                
                # Try alternative approach with truncate
                try:
                    print(f"Attempting TRUNCATE TABLE for {table}...")
                    cursor.execute(f"TRUNCATE TABLE {table}")
                    conn.commit()
                    print(f"Successfully truncated {table}")
                except Exception as e2:
                    conn.rollback()
                    print(f"TRUNCATE TABLE failed for {table}: {e2}")
        
        print("Database cleanup completed successfully!")
    
    except Exception as e:
        print(f"Error during database cleanup: {e}")
    
    finally:
        cursor.close()
        conn.close()

def manual_drop_constraints():
    """
    Manually drop and recreate all foreign key constraints.
    Use this if automatic cleanup fails.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("Starting constraint management...")
        
        # Get all foreign key constraints
        cursor.execute("""
            SELECT 
                fk.name AS FK_NAME,
                OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME
            FROM 
                sys.foreign_keys AS fk
        """)
        
        constraints = cursor.fetchall()
        
        # Drop all constraints
        for constraint in constraints:
            fk_name, table_name = constraint
            print(f"Dropping foreign key {fk_name} from table {table_name}")
            
            try:
                cursor.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {fk_name}")
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Error dropping constraint {fk_name}: {e}")
        
        # Now run the deletion
        cleanup_database()
        
        print("Manual constraint management completed.")
    
    except Exception as e:
        print(f"Error during constraint management: {e}")
    
    finally:
        cursor.close()
        conn.close()

def confirm_cleanup():
    """
    Ask for confirmation before cleaning up the database
    """
    print("WARNING: This will delete ALL data from the ESG database!")
    print("This cannot be undone. Make sure you have backups if needed.")
    
    confirmation = input("Type 'YES' to confirm deletion, or 'MANUAL' for manual constraint management: ")
    
    if confirmation == "YES":
        start_time = time.time()
        cleanup_database()
        end_time = time.time()
        print(f"Cleanup completed in {end_time - start_time:.2f} seconds")
    elif confirmation == "MANUAL":
        start_time = time.time()
        manual_drop_constraints()
        end_time = time.time()
        print(f"Manual cleanup completed in {end_time - start_time:.2f} seconds")
    else:
        print("Cleanup cancelled")

if __name__ == "__main__":
    confirm_cleanup() 