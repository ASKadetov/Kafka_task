import time
from random import choice, randrange
import psycopg2
import os
from dotenv import load_dotenv


# Загружаем переменные окружения
load_dotenv()
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('HOST')
DB_PORT = os.getenv('POSTGRES_PORT')


conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
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