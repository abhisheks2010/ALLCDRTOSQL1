import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to database
connection = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database='allcdr_dubaisouth'
)

cursor = connection.cursor()

print("=== DIM_DATE TABLE ===")
cursor.execute("SELECT date_key, full_date, year, quarter, month, day_of_week FROM dim_date ORDER BY date_key DESC LIMIT 10")
results = cursor.fetchall()

for row in results:
    print(f"date_key: {row[0]}, full_date: {row[1]}, year: {row[2]}, quarter: {row[3]}, month: {row[4]}, day_of_week: {row[5]}")

print("\n=== DIM_TIME_OF_DAY TABLE ===")
cursor.execute("SELECT time_key, full_time, hour, minute FROM dim_time_of_day ORDER BY time_key DESC LIMIT 10")
results = cursor.fetchall()

for row in results:
    print(f"time_key: {row[0]}, full_time: {row[1]}, hour: {row[2]}, minute: {row[3]}")

print("\n=== RECENT FACT_CALLS ===")
cursor.execute("SELECT call_id, date_key, time_key, call_recording_url FROM fact_calls ORDER BY call_key DESC LIMIT 5")
results = cursor.fetchall()

for row in results:
    print(f"call_id: {row[0]}, date_key: {row[1]}, time_key: {row[2]}, recording_url: {row[3]}")

cursor.close()
connection.close()