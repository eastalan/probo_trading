# Sports Events MySQL Database Setup

This guide explains how to set up and use the MySQL database for storing sports event data from FotMob and Melbet.

## Prerequisites

1. **MySQL Server** installed and running
2. **Python dependencies** installed:
   ```bash
   pip install PyMySQL cryptography
   ```

## Database Setup

### 1. Create Database and Tables

Run the SQL schema to create the database structure:

```bash
mysql -u root -p < database_schema.sql
```

Or manually execute the SQL commands:

```sql
-- Connect to MySQL
mysql -u root -p

-- Run the schema
source database_schema.sql;
```

### 2. Configure Database Connection

Edit `db_config.py` or set environment variables:

```bash
export DB_HOST="localhost"
export DB_PORT="3306"
export DB_USER="root"
export DB_PASSWORD="your_password"
export DB_NAME="sports_events"
```

## Usage

### Insert PSV Data

Run the database inserter to load your PSV files:

```bash
python3 db_inserter.py
```

This will:
- Connect to MySQL database
- Insert FotMob data from `data/event_data/fotmob_matches.psv`
- Insert Melbet data from `event_data/melbet_events.psv`
- Handle duplicates automatically
- Show insertion statistics

### Programmatic Usage

```python
from db_inserter import SportsDatabaseInserter
from db_config import DatabaseConfig

# Initialize inserter
inserter = SportsDatabaseInserter()

# Connect to database
if inserter.connect():
    # Insert specific files
    fotmob_count = inserter.insert_fotmob_data("data/event_data/fotmob_matches.psv")
    melbet_count = inserter.insert_melbet_data("event_data/melbet_events.psv")
    
    # Get statistics
    stats = inserter.get_event_stats()
    print(f"Total events: {stats['total_events']}")
    
    inserter.disconnect()
```

## Database Schema

### Main Tables

1. **`events`** - Main events table
   - `id` - Unique event ID
   - `source_id` - Reference to event source (FotMob/Melbet)
   - `match_name` - Full match name
   - `league` - League/competition name
   - `home_team`, `away_team` - Team names
   - `match_date`, `ko_time` - Match scheduling
   - `status`, `live_status` - Match status
   - `fotmob_id`, `melbet_id` - Source-specific IDs

2. **`event_sources`** - Source definitions
   - `fotmob`, `melbet`, `maxizone`

3. **`event_odds`** - Live betting odds
4. **`match_events`** - Live match events (goals, cards)
5. **`raw_data_logs`** - Raw data storage

### Views

- **`event_summary`** - Comprehensive event view with source names

## Data Format

### FotMob PSV Format
```
id|match_name|league|home_team|away_team|match_date|ko_time|status|live_status|match_url
```

### Melbet PSV Format
```
id|match_name|league|status|timestamp
```

## Querying Data

### Common Queries

```sql
-- Get all events from last 24 hours
SELECT * FROM event_summary 
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR);

-- Get events by league
SELECT * FROM events 
WHERE league LIKE '%Premier League%';

-- Get events from specific source
SELECT * FROM event_summary 
WHERE source = 'fotmob';

-- Count events by league
SELECT league, COUNT(*) as count 
FROM events 
GROUP BY league 
ORDER BY count DESC;
```

### Integration with Scrapers

You can modify your scrapers to insert data directly:

```python
# In your scraper
from db_inserter import SportsDatabaseInserter

inserter = SportsDatabaseInserter()
if inserter.connect():
    # After scraping, insert new data
    inserter.insert_fotmob_data("data/event_data/fotmob_matches.psv")
    inserter.disconnect()
```

## Monitoring

The database inserter provides:
- **Duplicate handling** - Updates existing records
- **Error logging** - Detailed logs in `logs/database/`
- **Statistics** - Event counts and summaries
- **Data validation** - Handles malformed PSV lines

## Troubleshooting

### Connection Issues
- Check MySQL server is running: `sudo systemctl status mysql`
- Verify credentials in `db_config.py`
- Test connection: `mysql -u root -p`

### Permission Issues
```sql
-- Grant permissions if needed
GRANT ALL PRIVILEGES ON sports_events.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```

### PSV File Issues
- Check file paths exist
- Verify PSV format matches expected structure
- Check logs in `logs/database/db_inserter.log`
