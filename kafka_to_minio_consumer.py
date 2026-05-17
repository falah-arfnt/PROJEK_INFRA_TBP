from kafka import KafkaConsumer
from minio import Minio
import json
import pandas as pd
from datetime import datetime
import time

# KAFKA CONSUMER
consumer = KafkaConsumer(
    'music-tracks',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MINIO CONNECTION
minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

bucket_name = "music-stream"


# CREATE BUCKET IF NOT EXISTS
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created")

# STORE STREAM DATA
stream_data = []
print("Consumer started...\n")


# READ STREAMING DATA
for message in consumer:
    data = message.value
    print("Received:", data)
    stream_data.append(data)

    # SAVE EVERY 20 RECORDS
    if len(stream_data) >= 20:
        df = pd.DataFrame(stream_data)
        filename = f"music_stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        local_path = filename

        # save locally
        df.to_csv(local_path, index=False)
        print(f"\nSaved locally: {filename}")

        # UPLOAD TO MINIO
        minio_client.fput_object(
            bucket_name,
            filename,
            local_path
        )

        print(f"Uploaded to MinIO: {filename}\n")
        stream_data = []