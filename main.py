import pandas as pd
import sqlalchemy
import requests
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from private import private_token, private_database, private_user_id


TOKEN = private_token
DATABASE = private_database
USER_ID = private_user_id


def check_date(df: pd.DataFrame):
# Check if dataframe is empty
    if df.empty:
        print('No new releases available ')


if __name__ == "__main__":
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

    release_df = pd.DataFrame(song_release, columns=['artist_name', 'release_date',
                                                     'song_name', 'album_type', 'total_tracks'])
    print(release_df.head())

    if check_date(release_df):
        print('Data valid')

    engine = sqlalchemy.create_engine(DATABASE)
    connection = engine.connect()

    Session = sessionmaker(bind=engine)
    session = Session()

    Base = declarative_base()


    class NewReleases(Base):
        __tablename__ = 'spotify_new_releases'

        song_name = Column(String, primary_key=True)
        artist_name = Column(String)
        release_date = Column(String)
        album_type = Column(String)
        total_tracks = Column(String)


    Base.metadata.create_all(engine)

    try:
        release_df.to_sql('spotify_new_releases', engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')

    session.commit()
    session.close()
    connection.close()
    print('Close database successfully')









