#!/usr/bin/env python3
"""
Create fotmob_events table in MySQL and import data from fotmob_matches.psv
"""
import pymysql
import csv
import os
import sys
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Amanpreet',  # Update with your MySQL password
    'database': 'sports_events',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True,
    'ssl_disabled': True
}

PSV_FILE_PATH = '/Users/aman/PycharmProjects/pythonProject/probo/data/event_data/fotmob_matches.psv'

def create_fotmob_events_table():
    """Create the fotmob_events table"""
    connection = pymysql.connect(**DB_CONFIG)
    
    try:
        with connection.cursor() as cursor:
            # Drop table if exists (for clean setup)
            cursor.execute("DROP TABLE IF EXISTS fotmob_events")
            
            # Create fotmob_events table based on PSV structure
            create_table_sql = """
            CREATE TABLE fotmob_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                match_date DATE NOT NULL,
                league_name VARCHAR(255) NOT NULL,
                home_team VARCHAR(255) NOT NULL,
                away_team VARCHAR(255) NOT NULL,
                kickoff_time VARCHAR(10),
                match_link TEXT,
                match_id VARCHAR(50) UNIQUE,
                uuid VARCHAR(255),
                download_flag TINYINT DEFAULT 0,
                has_ended TINYINT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_match_date (match_date),
                INDEX idx_league_name (league_name),
                INDEX idx_match_id (match_id),
                INDEX idx_uuid (uuid),
                INDEX idx_has_ended (has_ended)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            cursor.execute(create_table_sql)
            connection.commit()
            print("✅ fotmob_events table created successfully")
            
    finally:
        connection.close()

def import_psv_data():
    """Import data from fotmob_matches.psv into MySQL"""
    if not os.path.exists(PSV_FILE_PATH):
        print(f"❌ PSV file not found: {PSV_FILE_PATH}")
        return False
    
    connection = pymysql.connect(**DB_CONFIG)
    
    try:
        with connection.cursor() as cursor:
            imported_count = 0
            skipped_count = 0
            
            with open(PSV_FILE_PATH, 'r', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter='|')
                
                # Skip header row
                header = next(reader, None)
                if header:
                    print(f"📋 PSV Header: {header}")
                
                for row_num, row in enumerate(reader, 2):  # Start from row 2 (after header)
                    if len(row) < 10:  # Ensure we have all required columns including UUID
                        print(f"⚠️  Skipping row {row_num}: insufficient columns ({len(row)})")
                        skipped_count += 1
                        continue
                    
                    try:
                        # Parse data from PSV row
                        match_date_str = row[0].strip()
                        league_name = row[1].strip()
                        home_team = row[2].strip()
                        away_team = row[3].strip()
                        kickoff_time = row[4].strip() if row[4].strip() != 'N/A' else None
                        match_link = row[5].strip() if row[5].strip() != 'N/A' else None
                        match_id = row[6].strip() if row[6].strip() != 'N/A' else None
                        download_flag = int(row[7].strip()) if row[7].strip().isdigit() else 0
                        has_ended = int(row[8].strip()) if row[8].strip().isdigit() else 0
                        uuid = row[9].strip() if len(row) > 9 and row[9].strip() != 'N/A' else None
                        
                        # Convert date format from YYYYMMDD to YYYY-MM-DD
                        if len(match_date_str) == 8 and match_date_str.isdigit():
                            match_date = f"{match_date_str[:4]}-{match_date_str[4:6]}-{match_date_str[6:8]}"
                        else:
                            print(f"⚠️  Invalid date format in row {row_num}: {match_date_str}")
                            skipped_count += 1
                            continue
                        
                        # Insert into database
                        insert_sql = """
                        INSERT INTO fotmob_events 
                        (match_date, league_name, home_team, away_team, kickoff_time, 
                         match_link, match_id, uuid, download_flag, has_ended)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        league_name = VALUES(league_name),
                        home_team = VALUES(home_team),
                        away_team = VALUES(away_team),
                        kickoff_time = VALUES(kickoff_time),
                        match_link = VALUES(match_link),
                        uuid = VALUES(uuid),
                        download_flag = VALUES(download_flag),
                        has_ended = VALUES(has_ended),
                        updated_at = CURRENT_TIMESTAMP
                        """
                        
                        cursor.execute(insert_sql, (
                            match_date, league_name, home_team, away_team, 
                            kickoff_time, match_link, match_id, uuid, download_flag, has_ended
                        ))
                        
                        imported_count += 1
                        
                        if imported_count % 100 == 0:
                            print(f"📊 Imported {imported_count} records...")
                            
                    except Exception as e:
                        print(f"❌ Error processing row {row_num}: {e}")
                        print(f"   Row data: {row}")
                        skipped_count += 1
                        continue
            
            connection.commit()
            print(f"✅ Import completed: {imported_count} records imported, {skipped_count} skipped")
            
    finally:
        connection.close()
    
    return True

def verify_import():
    """Verify the imported data"""
    connection = pymysql.connect(**DB_CONFIG)
    
    try:
        with connection.cursor() as cursor:
            # Get total count
            cursor.execute("SELECT COUNT(*) as total FROM fotmob_events")
            total = cursor.fetchone()['total']
            print(f"📊 Total records in fotmob_events: {total}")
            
            # Get count by league
            cursor.execute("""
                SELECT league_name, COUNT(*) as count 
                FROM fotmob_events 
                GROUP BY league_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            leagues = cursor.fetchall()
            print("\n🏆 Top leagues by match count:")
            for league in leagues:
                print(f"  - {league['league_name']}: {league['count']} matches")
            
            # Get recent matches
            cursor.execute("""
                SELECT match_date, league_name, home_team, away_team, has_ended
                FROM fotmob_events 
                ORDER BY match_date DESC, id DESC 
                LIMIT 5
            """)
            recent = cursor.fetchall()
            print("\n📅 Recent matches:")
            for match in recent:
                status = "✅ Ended" if match['has_ended'] else "⏳ Upcoming"
                print(f"  - {match['match_date']}: {match['home_team']} vs {match['away_team']} ({match['league_name']}) {status}")
            
    finally:
        connection.close()

def main():
    """Main execution function"""
    print("🏗️  Creating fotmob_events table and importing data...")
    
    try:
        # Create table
        create_fotmob_events_table()
        
        # Import data
        if import_psv_data():
            # Verify import
            verify_import()
            print("\n🎉 fotmob_events table setup completed successfully!")
        else:
            print("❌ Data import failed")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
