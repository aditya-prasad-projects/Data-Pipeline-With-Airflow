class TestTables:
    TEST_SONGPLAYS_TABLE=[["""
    SELECT COUNT(*) FROM songplays WHERE level NOT IN ('free', 'paid')
    """, 0]]
    
    TEST_SONGS_TABLE=[["""
    SELECT COUNT(*) FROM songs WHERE songid IS NULL
    """, 0]]
    
    TEST_ARTISTS_TABLE=[["""
    SELECT COUNT(*) FROM artists WHERE artistid IS NULL
    """, 0]]
    
    TEST_USERS_TABLE=[["""
    SELECT COUNT(*) FROM users WHERE userid IS NULL
    """, 0]]
    
    TEST_TIME_TABLE=[["""
    SELECT COUNT(*) FROM "time" WHERE start_time IS NULL
    """, 0]]
    