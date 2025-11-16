from kafka import KafkaConsumer
import json
import clickhouse_connect
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
DB_USER = os.getenv('CLICKHOUSE_USER')
DB_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
DB_HOST = os.getenv('HOST')
DB_PORT = os.getenv('CLICKHOUSE_HTTP_PORT')
KAFKA_PORT = os.getenv('KAFKA_PORT')


consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers=f"localhost:{KAFKA_PORT}",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(host=DB_HOST, port=DB_PORT, username=DB_USER, password=DB_PASSWORD)

client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
""")

for message in consumer:
    data = message.value
    print("Received:", data)
    client.insert(
        'user_logins',
        [[data['user'], data['event'], int(data['timestamp'])]],
        column_names=['username', 'event_type', 'event_time']
    )