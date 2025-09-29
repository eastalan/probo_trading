#!/usr/bin/env python3
"""
FotMob Database Manager - Centralized database operations for FotMob programs
"""

import pandas as pd
from .config_loader import get_db_connection
import datetime
from typing import List, Dict, Optional, Tuple

class FotMobDBManager:
    """Manages database operations for FotMob events"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        if not self.conn:
            self.conn = get_db_connection()
            self.cursor = self.conn.cursor()
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    def get_matches_by_date(self, match_date: str) -> List[Dict]:
        """Get all matches for a specific date (YYYYMMDD format)"""
        self.connect()
        
        # Convert YYYYMMDD to YYYY-MM-DD
        try:
            date_obj = datetime.datetime.strptime(match_date, '%Y%m%d').date()
        except ValueError:
            return []
        
        self.cursor.execute('''
            SELECT match_id, league_name, home_team, away_team, kickoff_time, 
                   ko_datetime, match_link, uuid, download_flag, has_ended, created_at, updated_at, match_date
            FROM fotmob_events 
            WHERE match_date = %s
            ORDER BY ko_datetime
        ''', (date_obj,))
        
        results = self.cursor.fetchall()
        return [dict(row) for row in results]
    
    def get_processable_matches(self, match_date: str) -> List[Dict]:
        """Get matches that need processing (not ended, DownloadFlag 0 or 1)"""
        self.connect()
        
        try:
            date_obj = datetime.datetime.strptime(match_date, '%Y%m%d').date()
        except ValueError:
            return []
        
        self.cursor.execute('''
            SELECT match_id, league_name, home_team, away_team, kickoff_time, 
                   ko_datetime, match_link, uuid, download_flag, has_ended, match_date
            FROM fotmob_events 
            WHERE match_date = %s AND has_ended = 0 AND download_flag IN (0, 1)
            ORDER BY ko_datetime
        ''', (date_obj,))
        
        results = self.cursor.fetchall()
        return [dict(row) for row in results]
    
    def get_matches_in_time_window(self, start_datetime, end_datetime):
        """
        Get all matches within a specific datetime window
        """
        self.connect()
        
        query = '''
        SELECT match_id, league_name, home_team, away_team, kickoff_time, 
               ko_datetime, match_link, uuid, download_flag, has_ended, match_date
        FROM fotmob_events 
        WHERE ko_datetime BETWEEN %s AND %s
        ORDER BY ko_datetime
        '''
        
        try:
            self.cursor.execute(query, (start_datetime, end_datetime))
            results = self.cursor.fetchall()
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching matches in time window: {e}")
            return []
    
    def get_processable_matches_in_time_window(self, start_datetime, end_datetime):
        """
        Get processable matches within a specific datetime window
        Includes matches that need UUID extraction (uuid IS NULL or empty)
        """
        self.connect()
        
        query = '''
        SELECT match_id, league_name, home_team, away_team, kickoff_time, 
               ko_datetime, match_link, uuid, download_flag, has_ended, match_date
        FROM fotmob_events 
        WHERE ko_datetime BETWEEN %s AND %s
          AND has_ended = 0
        ORDER BY ko_datetime
        '''
        
        try:
            self.cursor.execute(query, (start_datetime, end_datetime))
            results = self.cursor.fetchall()
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching processable matches in time window: {e}")
            return []
    
    def get_upcoming_matches(self, from_datetime):
        """
        Get upcoming matches starting from a specific datetime
        """
        self.connect()
        
        query = '''
        SELECT match_id, league_name, home_team, away_team, kickoff_time, 
               ko_datetime, match_link, uuid, download_flag, has_ended, match_date
        FROM fotmob_events 
        WHERE ko_datetime > %s
          AND has_ended = 0
        ORDER BY ko_datetime
        '''
        
        try:
            self.cursor.execute(query, (from_datetime,))
            results = self.cursor.fetchall()
            return [dict(row) for row in results]
        except Exception as e:
            print(f"Error fetching upcoming matches: {e}")
            return []
    
    
    def update_match_uuid(self, match_id: str, uuid: str) -> bool:
        """Update UUID for a specific match"""
        self.connect()
        
        try:
            self.cursor.execute('''
                UPDATE fotmob_events 
                SET uuid = %s, updated_at = NOW()
                WHERE match_id = %s
            ''', (uuid, match_id))
            
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating UUID for match {match_id}: {e}")
            return False
    
    def update_download_flag(self, match_id: str, flag: int) -> bool:
        """Update download flag for a specific match"""
        self.connect()
        
        try:
            self.cursor.execute('''
                UPDATE fotmob_events 
                SET download_flag = %s, updated_at = NOW()
                WHERE match_id = %s
            ''', (flag, match_id))
            
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating download flag for match {match_id}: {e}")
            return False
    
    def update_has_ended(self, match_id: str, has_ended: int) -> bool:
        """Update has_ended flag for a specific match"""
        self.connect()
        
        try:
            self.cursor.execute('''
                UPDATE fotmob_events 
                SET has_ended = %s, updated_at = NOW()
                WHERE match_id = %s
            ''', (has_ended, match_id))
            
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating has_ended for match {match_id}: {e}")
            return False
    
    def bulk_update_expired_matches(self, match_date: str, hours_threshold: float = 3.5) -> int:
        """Mark matches as ended if they're past the time threshold"""
        self.connect()
        
        try:
            date_obj = datetime.datetime.strptime(match_date, '%Y%m%d').date()
        except ValueError:
            return 0
        
        try:
            # Get current time
            now = datetime.datetime.now()
            
            # Get matches that should be expired
            self.cursor.execute('''
                SELECT match_id, kickoff_time FROM fotmob_events 
                WHERE match_date = %s AND has_ended = 0 AND kickoff_time IS NOT NULL
            ''', (date_obj,))
            
            matches = self.cursor.fetchall()
            expired_count = 0
            
            for match in matches:
                try:
                    # Parse kickoff time
                    ko_dt = datetime.datetime.strptime(f'{match_date} {match["kickoff_time"]}', '%Y%m%d %H:%M')
                    
                    # Check if match is past threshold
                    if (now - ko_dt).total_seconds() > hours_threshold * 3600:
                        self.cursor.execute('''
                            UPDATE fotmob_events 
                            SET has_ended = 1, updated_at = NOW()
                            WHERE match_id = %s
                        ''', (match["match_id"],))
                        expired_count += 1
                except:
                    continue
            
            self.conn.commit()
            return expired_count
            
        except Exception as e:
            print(f"Error bulk updating expired matches: {e}")
            return 0
    
    def reset_invalid_uuids(self, match_date: str) -> int:
        """Reset matches with invalid UUIDs so they can be reprocessed"""
        self.connect()
        
        try:
            date_obj = datetime.datetime.strptime(match_date, '%Y%m%d').date()
        except ValueError:
            return 0
        
        try:
            self.cursor.execute('''
                UPDATE fotmob_events 
                SET download_flag = 0, updated_at = NOW()
                WHERE match_date = %s AND download_flag = 1 AND has_ended = 0 
                AND (uuid IS NULL OR uuid = '' OR uuid = 'nan' OR uuid = 'NaN')
            ''', (date_obj,))
            
            self.conn.commit()
            return self.cursor.rowcount
            
        except Exception as e:
            print(f"Error resetting invalid UUIDs: {e}")
            return 0
    
    def get_match_by_id(self, match_id: str) -> Optional[Dict]:
        """Get a specific match by ID"""
        self.connect()
        
        self.cursor.execute('''
            SELECT match_id, league_name, home_team, away_team, kickoff_time, 
                   match_link, uuid, download_flag, has_ended, created_at, updated_at
            FROM fotmob_events 
            WHERE match_id = %s
        ''', (match_id,))
        
        result = self.cursor.fetchone()
        return dict(result) if result else None
    
    def sync_from_psv(self, psv_file_path: str) -> Tuple[int, int, int]:
        """Sync data from PSV file to database"""
        self.connect()
        
        # Read PSV data
        df = pd.read_csv(psv_file_path, delimiter='|', dtype=str)
        
        inserted = 0
        updated = 0
        errors = 0
        
        for _, row in df.iterrows():
            try:
                # Convert date format from YYYYMMDD to YYYY-MM-DD
                try:
                    match_date = datetime.datetime.strptime(row['MatchDate'], '%Y%m%d').date()
                except:
                    errors += 1
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
                self.cursor.execute('SELECT id FROM fotmob_events WHERE match_id = %s', (row['MatchID'],))
                existing = self.cursor.fetchone()
                
                if existing:
                    # Calculate ko_datetime for update
                    ko_datetime = None
                    if kickoff_time:
                        try:
                            # Try parsing as 12-hour format first
                            ko_datetime = datetime.datetime.strptime(f"{match_date.strftime('%Y-%m-%d')} {kickoff_time}", '%Y-%m-%d %I:%M %p')
                        except ValueError:
                            try:
                                # Try parsing as 24-hour format
                                ko_datetime = datetime.datetime.strptime(f"{match_date.strftime('%Y-%m-%d')} {kickoff_time}", '%Y-%m-%d %H:%M')
                            except ValueError:
                                pass
                    
                    # Update existing record
                    self.cursor.execute('''
                        UPDATE fotmob_events SET 
                            match_date = %s, league_name = %s, home_team = %s, away_team = %s,
                            kickoff_time = %s, ko_datetime = %s, match_link = %s, uuid = %s, 
                            download_flag = %s, has_ended = %s, updated_at = NOW()
                        WHERE match_id = %s
                    ''', (
                        match_date, row['LeagueName'], row['HomeTeam'], row['AwayTeam'],
                        kickoff_time, ko_datetime, match_link, uuid_val,
                        int(row['DownloadFlag']) if row['DownloadFlag'].isdigit() else 0,
                        int(row['HasEnded']) if row['HasEnded'].isdigit() else 0,
                        row['MatchID']
                    ))
                    updated += 1
                else:
                    # Calculate ko_datetime for new record
                    ko_datetime = None
                    if kickoff_time:
                        try:
                            # Try parsing as 12-hour format first
                            ko_datetime = datetime.datetime.strptime(f"{match_date.strftime('%Y-%m-%d')} {kickoff_time}", '%Y-%m-%d %I:%M %p')
                        except ValueError:
                            try:
                                # Try parsing as 24-hour format
                                ko_datetime = datetime.datetime.strptime(f"{match_date.strftime('%Y-%m-%d')} {kickoff_time}", '%Y-%m-%d %H:%M')
                            except ValueError:
                                pass
                    
                    # Insert new record
                    self.cursor.execute('''
                        INSERT INTO fotmob_events 
                        (match_date, league_name, home_team, away_team, kickoff_time, ko_datetime, match_link, match_id, uuid, download_flag, has_ended)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        match_date, row['LeagueName'], row['HomeTeam'], row['AwayTeam'],
                        kickoff_time, ko_datetime, match_link, row['MatchID'], uuid_val,
                        int(row['DownloadFlag']) if row['DownloadFlag'].isdigit() else 0,
                        int(row['HasEnded']) if row['HasEnded'].isdigit() else 0
                    ))
                    inserted += 1
                    
            except Exception as e:
                print(f"Error processing row {row['MatchID']}: {e}")
                errors += 1
                continue
        
        self.conn.commit()
        return inserted, updated, errors

# Convenience functions for backward compatibility
def get_db_matches_by_date(match_date: str) -> List[Dict]:
    """Get matches for a specific date"""
    with FotMobDBManager() as db:
        return db.get_matches_by_date(match_date)

def update_match_uuid_in_db(match_id: str, uuid: str) -> bool:
    """Update UUID for a match in database"""
    with FotMobDBManager() as db:
        return db.update_match_uuid(match_id, uuid)

def update_download_flag_in_db(match_id: str, flag: int) -> bool:
    """Update download flag for a match in database"""
    with FotMobDBManager() as db:
        return db.update_download_flag(match_id, flag)
