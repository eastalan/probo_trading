#!/usr/bin/env python3
"""
Sync fotmob_matches.psv data to database fotmob_events table
"""

import pandas as pd
from .config_loader import get_db_connection
import datetime
import sys

def sync_psv_to_database():
    """Sync PSV file data to database"""
    
    # Read PSV data
    df = pd.read_csv('data/event_data/fotmob_matches.psv', delimiter='|', dtype=str)
    print(f'PSV file has {len(df)} records')

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check current database records
    cursor.execute('SELECT COUNT(*) FROM fotmob_events')
    db_count = cursor.fetchone()['COUNT(*)']
    print(f'Database has {db_count} records')

    # Insert/update records from PSV to database
    inserted = 0
    updated = 0
    errors = 0

    for _, row in df.iterrows():
        try:
            # Convert date format from YYYYMMDD to YYYY-MM-DD
            try:
                match_date = datetime.datetime.strptime(row['MatchDate'], '%Y%m%d').date()
            except:
                print(f"Skipping row with invalid date: {row['MatchDate']}")
                continue
            
            # Handle UUID - convert nan/empty to None
            uuid_val = None
            if pd.notna(row['UUID']):
                uuid_str = str(row['UUID']).strip()
                if uuid_str not in ['', 'nan', 'NaN']:
                    uuid_val = uuid_str
            
            # Handle KickOffTime - convert nan to None
            kickoff_time = None
            if pd.notna(row['KickOffTime']):
                kickoff_str = str(row['KickOffTime']).strip()
                if kickoff_str not in ['', 'nan', 'NaN']:
                    kickoff_time = kickoff_str
            
            # Handle MatchLink - convert nan to None  
            match_link = None
            if pd.notna(row['MatchLink']):
                link_str = str(row['MatchLink']).strip()
                if link_str not in ['', 'nan', 'NaN']:
                    match_link = link_str
            
            # Check if record exists
            cursor.execute('SELECT id FROM fotmob_events WHERE match_id = %s', (row['MatchID'],))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing record
                cursor.execute('''
                    UPDATE fotmob_events SET 
                        match_date = %s, league_name = %s, home_team = %s, away_team = %s,
                        kickoff_time = %s, match_link = %s, uuid = %s, 
                        download_flag = %s, has_ended = %s, updated_at = NOW()
                    WHERE match_id = %s
                ''', (
                    match_date, row['LeagueName'], row['HomeTeam'], row['AwayTeam'],
                    kickoff_time, match_link, uuid_val,
                    int(row['DownloadFlag']) if row['DownloadFlag'].isdigit() else 0,
                    int(row['HasEnded']) if row['HasEnded'].isdigit() else 0,
                    row['MatchID']
                ))
                updated += 1
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO fotmob_events 
                    (match_date, league_name, home_team, away_team, kickoff_time, match_link, match_id, uuid, download_flag, has_ended)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    match_date, row['LeagueName'], row['HomeTeam'], row['AwayTeam'],
                    kickoff_time, match_link, row['MatchID'], uuid_val,
                    int(row['DownloadFlag']) if row['DownloadFlag'].isdigit() else 0,
                    int(row['HasEnded']) if row['HasEnded'].isdigit() else 0
                ))
                inserted += 1
                
        except Exception as e:
            print(f"Error processing row {row['MatchID']}: {e}")
            errors += 1
            continue

    conn.commit()
    print(f'Inserted {inserted} new records, updated {updated} existing records, {errors} errors')

    # Verify final count
    cursor.execute('SELECT COUNT(*) FROM fotmob_events')
    final_count = cursor.fetchone()['COUNT(*)']
    print(f'Database now has {final_count} total records')

    conn.close()
    return inserted, updated, errors

if __name__ == "__main__":
    sync_psv_to_database()
