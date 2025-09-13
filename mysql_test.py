#!/usr/bin/env python3
"""
Simple MySQL connection test
"""
import pymysql
import sys

def test_mysql_connection():
    """Test MySQL connection and create basic table"""
    try:
        # Connect to MySQL
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='password123',  # Change this to your actual password
            database='sports_events',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        print("✅ Successfully connected to MySQL!")
        
        with connection.cursor() as cursor:
            # Create a simple test table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_events (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    event_name VARCHAR(255) NOT NULL,
                    source VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert test data
            cursor.execute("""
                INSERT INTO test_events (event_name, source) 
                VALUES (%s, %s)
            """, ('Test Match', 'fotmob'))
            
            connection.commit()
            
            # Query data
            cursor.execute("SELECT * FROM test_events")
            results = cursor.fetchall()
            
            print(f"📊 Found {len(results)} records in test_events table:")
            for row in results:
                print(f"  - ID: {row['id']}, Event: {row['event_name']}, Source: {row['source']}")
        
        connection.close()
        print("🔌 Connection closed successfully")
        
    except Exception as e:
        print(f"❌ Error connecting to MySQL: {e}")
        print("\n💡 Make sure to:")
        print("1. Update the password in this script")
        print("2. MySQL server is running: brew services start mysql")
        print("3. Database 'sports_events' exists")
        return False
    
    return True

if __name__ == "__main__":
    print("🔍 Testing MySQL connection...")
    success = test_mysql_connection()
    sys.exit(0 if success else 1)
