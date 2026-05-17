import requests
import pandas as pd

API_KEY = "00f44cd8c282a4e5c2a4d9f79aeac2c8"

url = f"http://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key={API_KEY}&format=json"

response = requests.get(url)

data = response.json()

tracks = data['tracks']['track']

result = []

for track in tracks:
    result.append({
        'name': track['name'],
        'artist': track['artist']['name'],
        'listeners': track['listeners'],
        'playcount': track['playcount']
    })

df = pd.DataFrame(result)

print(df.head())

df.to_csv("top_tracks.csv", index=False)

print("Data berhasil disimpan!")