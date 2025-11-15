import time
from random import choice, randrange
import psycopg2


conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
)
""")
conn.commit()

def send_generated_data(n: int):
    users = ["alice", "bob", "carol", "dave"]
    data = sorted(
        [(choice(users), "login", time.time() + randrange(1000)) for _ in range(n)],
        key=lambda x: x[-1]
    )
    
    cursor.executemany(
        "INSERT INTO user_logins (username, event_type, event_time) VALUES (%s, %s, to_timestamp(%s))", 
        data
    )
    conn.commit()
    print(f"Sent to db: {len(data)} rows")
        
send_generated_data(200)

cursor.close()
conn.close()