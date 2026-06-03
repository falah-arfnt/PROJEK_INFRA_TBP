import pandas as pd
from minio import Minio
import io

print("⚡ Membaca data music_data.json...")

df = pd.read_json('music_data.json', lines=True)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df['hour'] = df['timestamp'].dt.hour

print("Memproses agregasi data...")

# A. Top 10 Songs 
top_songs = df.groupby(['song', 'artist']).agg({'playcount': 'max', 'listeners': 'max'}).sort_values(by='playcount', ascending=False).reset_index().head(10)

# B. Top 10 Artists
top_artists = df.groupby('artist').agg({'playcount': 'max'}).sort_values(by='playcount', ascending=False).reset_index().head(10)

# C. Trend per Jam 
hourly_trend = df.groupby('hour').size().reset_index(name='total_activities')

# D. Engagement Rate 
df['engagement'] = df['playcount'] / df['listeners']
engagement_ranking = df.groupby(['song', 'artist']).agg({'engagement': 'mean'}).sort_values(by='engagement', ascending=False).reset_index().head(10)


print("Menghubungkan ke MinIO Data Lake...")


minio_client = Minio(
    "localhost:9000", 
    access_key="admin", 
    secret_key="password123", 
    secure=False
)

bucket_name = "music-analytics"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' berhasil dibuat.")

def upload_to_minio(dataframe, object_name):
    csv_bytes = dataframe.to_csv(index=False).encode('utf-8')
    minio_client.put_object(
        bucket_name,
        object_name,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type='application/csv'
    )
    print(f"✅ Berhasil mengunggah {object_name} ke MinIO!")

upload_to_minio(top_songs, "historical/top_songs.csv")
upload_to_minio(top_artists, "historical/top_artists.csv")
upload_to_minio(hourly_trend, "historical/hourly_trend.csv")
upload_to_minio(engagement_ranking, "historical/engagement_ranking.csv")

print("Batch processing selesai!")