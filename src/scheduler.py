import schedule
import time
import mysql.connector
from ingestion import ingest_data
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

def run_transformations():
    """Run SQL transformations"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Running transformations...")

        # Get project root directory (parent of src folder)
        current_dir = os.path.dirname(os.path.abspath(__file__))  # src folder
        project_root = os.path.dirname(current_dir)  # parent folder
        sql_file_path = os.path.join(project_root, 'sql', 'transformations.sql')
        
        # Read SQL file and execute commands
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # Split by semicolon to execute each command
        sql_commands = sql_content.split(';')
        
        for command in sql_commands:
            command = command.strip()
            if command:  # Skip empty commands
                cursor.execute(command)
        
        conn.commit()
        print("Transformations completed successfully!")
        
    except Exception as e:
        print(f"Transformation error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def run_full_pipeline():
    """Run complete ETL pipeline"""
    print("\n" + "="*60)
    print("STARTING ETL PIPELINE")
    print("="*60)
    
    # Step 1: Ingest data from API
    print("\n[Step 1/2] Ingesting data from OpenWeatherMap API...")
    ingest_data()
    
    # Step 2: Transform data
    print("\n[Step 2/2] Transforming data into fact table...")
    run_transformations()
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETED!")
    print("="*60 + "\n")

# Schedule to run every 6 hours
schedule.every(6).hours.do(run_full_pipeline)

if __name__ == "__main__":
    print("SCHEDULER STARTED!")
    print("Pipeline will run automatically every 6 hours")
    print("Press Ctrl+C to stop\n")
    
    # Run once immediately
    run_full_pipeline()
    
    # Then run on schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute