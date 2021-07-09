CREATE TABLE IF NOT EXISTS spotify_tracks (
    song_id SERIAL PRIMARY KEY,
    artist_name VARCHAR NOT NULL,
    release_date DATE NOT NULL,
    song_name VARCHAR NOT NULL,
    album_type VARCHAR NOT NULL,
    total_tracks INT NOT NULL,
    UNIQUE (song_id, release_date)
);



