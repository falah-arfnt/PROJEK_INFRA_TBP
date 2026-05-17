import pandas as pd
import random
import time
import json
from datetime import datetime

from kafka import KafkaProducer

df = pd.read_csv(
    r"D:\INFRA_BIG_DATA\PROJEK_MINIO\music-trend-pipeline\top_tracks.csv",
    sep=';'
)
print("Kolom dataset:")
print(df.columns)
print("\nStreaming started...\n")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    sample = df.sample(1).iloc[0]

    try:
        listeners = int(sample['listeners'])
        playcount = int(sample['playcount'])
    except:
        print("Skipped broken row")
        continue

    listeners += random.randint(1, 1000)
    playcount += random.randint(100, 5000)
    message = {

        "song": sample['name'],
        "artist": sample['artist'],
        "listeners": listeners,
        "playcount": playcount,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    }

    # KIRIM KE KAFKA
    producer.send(
        'music-tracks',
        value=message
    )
    producer.flush()
    print("Sent:", message)
    time.sleep(5)