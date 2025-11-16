import psycopg2
from kafka import KafkaProducer
import json
import time
import os
from dotenv import load_dotenv


# Загружаем переменные окружения
load_dotenv()
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
KAFKA_PORT = os.getenv('KAFKA_PORT')

producer = KafkaProducer(
    bootstrap_servers=f'localhost:{KAFKA_PORT}',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

cursor.execute(
    """
    SELECT id, username, event_type, extract(epoch FROM event_time) 
    FROM user_logins
    WHERE sent_to_kafka IS FALSE
    """
)
rows = cursor.fetchall()

for row in rows:
    id = row[0]
    data = {
        "user": row[1],
        "event": row[2],
        "timestamp": float(row[3])
    }
    producer.send("user_events", value=data)
    cursor.execute(f"UPDATE user_logins SET sent_to_kafka = TRUE WHERE id = {id}")
    conn.commit()
    print("Sent:", data)
    time.sleep(0.2)