import duckdb

conn = duckdb.connect()
conn.execute('INSTALL httpfs; LOAD httpfs;')
conn.execute('INSTALL delta; LOAD delta;')

try:
    conn.execute('''
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    ''')
    res = conn.execute("SELECT * FROM delta_scan('s3://silver/test_delta')").fetchall()
    print('SUCCESS:', res)
except Exception as e:
    print('ERROR:', e)
