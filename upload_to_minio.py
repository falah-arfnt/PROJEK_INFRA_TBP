from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

bucket_name = "music-data"

if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

client.fput_object(
    bucket_name,
    "raw/top_tracks.csv",
    "top_tracks.csv"
)

print("file e redi di minio lurrr")