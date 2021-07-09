import requests
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from private import private_token, private_database, private_user_id


def get_tracks(**context):
    TOKEN = private_token

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {TOKEN}"
    }

    req = requests.get(f'https://api.spotify.com/v1/browse/new-releases?country=RU&limit=50', headers=headers)

    data = req.json()

    artist_name = []
    release_date = []
    song_name = []
    album_type = []
    total_tracks = []

    for release in data['albums']['items']:
        artist_name.append(release['artists'][0]['name'])
        release_date.append(release['release_date'])
        song_name.append(release['name'])
        album_type.append(release['album_type'])
        total_tracks.append(release['total_tracks'])

    song_release = {
        'artist_name': artist_name,
        'release_date': release_date,
        'song_name': song_name,
        'album_type': album_type,
        'total_tracks': total_tracks,
    }

    context['ti'].xcom_push(key='song_release', value=song_release)

def save_tracks_to_db(**context):
    DATABASE = private_database
    engine = sqlalchemy.create_engine(DATABASE)
    connection = engine.connect()

    Session = sessionmaker(bind=engine)
    session = Session()

    song_release = context['ti'].xcom_pull(task_ids='getting_data', key='song_release')

    release_df = pd.DataFrame(song_release, columns=['artist_name', 'release_date',
                                                     'song_name', 'album_type', 'total_tracks'])
    release_df.to_sql('spotify_tracks', engine, index=False, if_exists='replace')

    session.commit()
    session.close()
    connection.close()


