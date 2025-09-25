import mysql.connector
try:
    conn = mysql.connector.connect(host='localhost', user='root', password='Support@Multy123.com')
    cursor = conn.cursor()
    cursor.execute("CREATE USER IF NOT EXISTS 'cdruser'@'localhost' IDENTIFIED BY 'Support@Multy123.com'")
    cursor.execute('CREATE DATABASE IF NOT EXISTS allcdr_shams')
    cursor.execute('CREATE DATABASE IF NOT EXISTS allcdr_spc')
    cursor.execute('CREATE DATABASE IF NOT EXISTS allcdr_dubaisouth')
    cursor.execute("GRANT ALL PRIVILEGES ON allcdr_shams.* TO 'cdruser'@'localhost'")
    cursor.execute("GRANT ALL PRIVILEGES ON allcdr_spc.* TO 'cdruser'@'localhost'")
    cursor.execute("GRANT ALL PRIVILEGES ON allcdr_dubaisouth.* TO 'cdruser'@'localhost'")
    cursor.execute('FLUSH PRIVILEGES')
    conn.commit()
    print('User and databases created successfully.')
    cursor.close()
    conn.close()
except Exception as e:
    print(f'Error: {e}')