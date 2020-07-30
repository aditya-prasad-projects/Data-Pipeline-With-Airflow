class AnalysisQueries:
    USER_SONG_POPULARITY_INDEPTH="""DROP TABLE IF EXISTS {destination_table};
        Create TABLE {destination_table} AS
        (SELECT userid, level, songs.title, COUNT(songplays.songid) as times_played
        FROM songplays
        JOIN songs
        ON songplays.songid = songs.songid
        GROUP BY songs.title, userid, level
        ORDER BY userid desc);
    """
    
    USER_SONG_POPULARITY_SUMMARY="""DROP TABLE IF EXISTS {destination_table};
        Create TABLE {destination_table} AS
        ( SELECT songplays.userid, songplays.level, COUNT(songplays.songid) as songs_played
        FROM songplays
        GROUP BY songplays.userid, songplays.level
        ORDER BY songplays.userid);
       """