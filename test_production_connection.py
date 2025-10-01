#!/usr/bin/env python3
"""
Simple production connection test for MySQL credentials
"""

import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def test_production_connection():
    """Test different credential combinations"""
    
    databases = ['allcdr_shams', 'allcdr_spc', 'allcdr_meydan', 'allcdr_dubaisouth']
    
    # Different credential sets to try
    credential_sets = [
        {
            'name': 'ShamsUser (Production)',
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': 'ShamsUser',
            'password': '$MJR732o}jVz[?PN'
        },
        {
            'name': 'ENV Credentials',
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD')
        },
        {
            'name': 'Root with rootpass',
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': 'root',
            'password': 'rootpass'
        },
        {
            'name': 'Root with Support password',
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': 'root',
            'password': 'Support@Multy123.com'
        }
    ]
    
    print("🔐 PRODUCTION CONNECTION TEST")
    print("="*60)
    
    for cred in credential_sets:
        print(f"\n🧪 Testing: {cred['name']}")
        print(f"   Host: {cred['host']}")
        print(f"   User: {cred['user']}")
        print(f"   Password: {'*' * len(cred['password'])}")
        
        for db_name in databases:
            try:
                connection = mysql.connector.connect(
                    host=cred['host'],
                    user=cred['user'],
                    password=cred['password'],
                    database=db_name,
                    connection_timeout=5
                )
                
                cursor = connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM dim_date WHERE year < 2000 OR year > 2030")
                invalid_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM dim_date")
                total_count = cursor.fetchone()[0]
                
                print(f"   ✅ {db_name}: {invalid_count} invalid / {total_count} total")
                
                cursor.close()
                connection.close()
                
            except mysql.connector.Error as e:
                print(f"   ❌ {db_name}: {e}")
        
        # If this credential set worked for at least one database, use it
        working_count = 0
        for db_name in databases:
            try:
                connection = mysql.connector.connect(
                    host=cred['host'],
                    user=cred['user'],
                    password=cred['password'],
                    database=db_name,
                    connection_timeout=5
                )
                connection.close()
                working_count += 1
            except:
                pass
        
        if working_count > 0:
            print(f"\n🎯 RECOMMENDATION: Use '{cred['name']}' (works for {working_count}/{len(databases)} databases)")
            break
    
    print(f"\n💡 NEXT STEPS:")
    print("1. Update your .env file with working credentials")
    print("2. Or modify production_fix_dates.py to use the working credentials")
    print("3. Re-run the fix script")

if __name__ == "__main__":
    test_production_connection()