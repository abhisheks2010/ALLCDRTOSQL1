#!/usr/bin/env python3
"""
Production fix script for invalid date entries in dim_date table.
This script safely handles foreign key constraints.
"""

import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def fix_production_dates():
    """
    Safe fix for production - handles foreign key constraints properly
    """
    
    databases = ['allcdr_shams', 'allcdr_spc', 'allcdr_meydan', 'allcdr_dubaisouth']
    
    print("🏥 PRODUCTION DATE FIX SCRIPT")
    print("="*50)
    
    for db_name in databases:
        try:
            print(f"\n🔧 Processing {db_name}...")
            
            connection = mysql.connector.connect(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=db_name
            )
            
            cursor = connection.cursor()
            
            # Step 1: Check current state
            cursor.execute("SELECT COUNT(*) FROM dim_date WHERE year < 2000 OR year > 2030")
            invalid_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fact_calls")
            fact_calls_count = cursor.fetchone()[0]
            
            print(f"   📊 Invalid dim_date entries: {invalid_count}")
            print(f"   📊 Total fact_calls: {fact_calls_count}")
            
            if invalid_count == 0:
                print(f"   ✅ No invalid dates found in {db_name}")
                cursor.close()
                connection.close()
                continue
            
            # Step 2: Check if invalid dates are referenced by fact_calls
            cursor.execute("""
                SELECT dd.date_key, dd.full_date, dd.year, COUNT(fc.call_key) as fact_count
                FROM dim_date dd
                LEFT JOIN fact_calls fc ON dd.date_key = fc.date_key
                WHERE dd.year < 2000 OR dd.year > 2030
                GROUP BY dd.date_key, dd.full_date, dd.year
            """)
            
            invalid_refs = cursor.fetchall()
            
            print(f"   📋 Invalid date references:")
            total_affected_calls = 0
            for date_key, full_date, year, fact_count in invalid_refs:
                print(f"      {date_key} ({year}) -> {fact_count} fact_calls")
                total_affected_calls += fact_count
            
            if total_affected_calls > 0:
                print(f"   ⚠️  {total_affected_calls} fact_calls would be affected")
                print(f"   🔄 SOLUTION: Update fact_calls to use current date_key")
                
                # Create a current date entry if it doesn't exist
                current_date = datetime.now().date()
                current_date_key = int(current_date.strftime('%Y%m%d'))
                
                # Insert current date dimension if not exists
                insert_current_date_sql = """
                INSERT IGNORE INTO dim_date (date_key, full_date, year, quarter, month, day_of_week)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                quarter = (current_date.month - 1) // 3 + 1
                day_of_week = current_date.strftime('%A')
                
                cursor.execute(insert_current_date_sql, (
                    current_date_key, current_date, current_date.year,
                    quarter, current_date.month, day_of_week
                ))
                
                print(f"   ✅ Ensured current date entry exists: {current_date_key}")
                
                # Update fact_calls to reference current date instead of invalid dates
                for date_key, _, year, fact_count in invalid_refs:
                    if fact_count > 0:
                        cursor.execute("""
                            UPDATE fact_calls 
                            SET date_key = %s 
                            WHERE date_key = %s
                        """, (current_date_key, date_key))
                        
                        print(f"   🔄 Updated {cursor.rowcount} fact_calls from {date_key} to {current_date_key}")
                
                connection.commit()
                
                # Now we can safely delete the invalid date entries
                cursor.execute("DELETE FROM dim_date WHERE year < 2000 OR year > 2030")
                deleted_count = cursor.rowcount
                connection.commit()
                
                print(f"   🗑️  Deleted {deleted_count} invalid dim_date entries")
                
            else:
                # No references, safe to delete directly
                cursor.execute("DELETE FROM dim_date WHERE year < 2000 OR year > 2030")
                deleted_count = cursor.rowcount
                connection.commit()
                print(f"   🗑️  Deleted {deleted_count} invalid dim_date entries (no references)")
            
            # Step 3: Final verification
            cursor.execute("SELECT COUNT(*) FROM dim_date WHERE year < 2000 OR year > 2030")
            remaining_invalid = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_date")
            total_dates = cursor.fetchone()[0]
            
            print(f"   ✅ Final state: {remaining_invalid} invalid, {total_dates} total dim_date entries")
            
            cursor.close()
            connection.close()
            
        except Exception as e:
            print(f"   ❌ Error processing {db_name}: {e}")

def verify_fix():
    """Verify the fix worked across all databases"""
    
    databases = ['allcdr_shams', 'allcdr_spc', 'allcdr_meydan', 'allcdr_dubaisouth']
    
    print(f"\n📋 VERIFICATION REPORT")
    print("="*50)
    
    total_invalid = 0
    
    for db_name in databases:
        try:
            connection = mysql.connector.connect(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=db_name
            )
            
            cursor = connection.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM dim_date WHERE year < 2000 OR year > 2030")
            invalid_count = cursor.fetchone()[0]
            total_invalid += invalid_count
            
            cursor.execute("SELECT COUNT(*) FROM dim_date")
            total_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT MIN(year), MAX(year) FROM dim_date")
            year_range = cursor.fetchone()
            
            status = "✅ CLEAN" if invalid_count == 0 else "❌ ISSUES"
            print(f"{db_name:20} {status:10} {invalid_count:3} invalid / {total_count:3} total (years: {year_range[0]}-{year_range[1]})")
            
            cursor.close()
            connection.close()
            
        except Exception as e:
            print(f"{db_name:20} ❌ ERROR    {str(e)[:50]}")
    
    print("="*50)
    print(f"TOTAL INVALID ENTRIES REMAINING: {total_invalid}")
    
    if total_invalid == 0:
        print("🎉 ALL DATABASES ARE CLEAN!")
    else:
        print("⚠️  SOME ISSUES REMAIN - CHECK LOGS ABOVE")

if __name__ == "__main__":
    print("🚀 Starting production dim_date fix...")
    fix_production_dates()
    verify_fix()
    
    print(f"\n💡 NEXT STEPS FOR PRODUCTION:")
    print("="*50)
    print("1. Copy this script to your production server")
    print("2. Update .env file paths if needed")
    print("3. Run: python production_fix_dates.py")
    print("4. Test ETL pipeline with fixed timestamp logic")
    print("5. Monitor logs for proper date conversions")
    print("\nThe ETL will now use current time instead of invalid dates!")