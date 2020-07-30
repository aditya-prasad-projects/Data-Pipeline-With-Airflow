from helpers.create_tables import CreateTables
from helpers.load_tables import LoadTables
from helpers.test_tables import TestTables
from helpers.analysis_queries import AnalysisQueries

class SQLQueries:
    create_load_test_queries = {
        "songplays": {
            "CREATE": CreateTables.CREATE_SONGPLAYS_TABLE_SQL,
            "LOAD": LoadTables.SONGPLAY_TABLE_INSERT_SQL,
            "TEST": TestTables.TEST_SONGPLAYS_TABLE
        },
        "songs": {
            "CREATE": CreateTables.CREATE_SONGS_TABLE_SQL,
            "LOAD": LoadTables.SONG_TABLE_INSERT_SQL,
            "TEST": TestTables.TEST_SONGS_TABLE
        },
        "artists": {
            "CREATE": CreateTables.CREATE_ARTISTS_TABLE_SQL,
            "LOAD": LoadTables.ARTIST_TABLE_INSERT_SQL,
            "TEST": TestTables.TEST_ARTISTS_TABLE
        },
        "users": {
            "CREATE": CreateTables.CREATE_USERS_TABLE_SQL,
            "LOAD": LoadTables.USER_TABLE_INSERT_SQL,
            "TEST": TestTables.TEST_USERS_TABLE
        },
        "time": {
            "CREATE": CreateTables.CREATE_TIME_TABLE_SQL,
            "LOAD": LoadTables.TIME_TABLE_INSERT_SQL,
            "TEST": TestTables.TEST_TIME_TABLE
        },
        "staging_songs": {
            "CREATE": CreateTables.CREATE_STAGING_SONGS_TABLE_SQL,
            "LOAD": LoadTables.LOAD_STAGING_TABLE_SQL
        },
        "staging_events": {
            "CREATE": CreateTables.CREATE_STAGING_EVENTS_TABLE_SQL,
            "LOAD": LoadTables.LOAD_STAGING_TABLE_SQL
        }
    }
    
    data_quality_check_queries = "SELECT COUNT(*) FROM {table}"
    
    truncate_sql = "TRUNCATE {table}"
    
    analysis_sql = {
        "USER_SONG_POPULARITY_INDEPTH": AnalysisQueries.USER_SONG_POPULARITY_INDEPTH,
        "USER_SONG_POPULARITY_SUMMARY": AnalysisQueries.USER_SONG_POPULARITY_SUMMARY
    }
        
        
        