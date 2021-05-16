#!/usr/bin/env python
# coding: utf-8
#from sql_queries import create_table_queries, drop_table_queries

def run_etl():
    """
    main function which processes the drop and creation of tables
    """
    import configparser
    import configparser
    import psycopg2

    staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS song_stage                                 (num_songs int,                                 artist_id varchar,                                 artist_latitude varchar,                                 artist_longitude varchar,                                 artist_location varchar,                                 artist_name varchar,                                 song_name varchar,                                 song_id varchar,                                 title varchar,                                 duration float,                                 year int)
    """)

    config = configparser.ConfigParser()


    staging_songs_copy = ("""
            COPY song_stage FROM 's3://deepamdataengineering/songs/'
            access_key_id '<xyz>' secret_access_key '<abc>'
            FORMAT AS JSON 'auto' REGION 'us-east-1'
        """)

    DWH_DB='testdb'
    print(DWH_DB)
    DWH_DB_USER= 'deepam'
    print(DWH_DB_USER)
    DWH_DB_PASSWORD= 'Deepam0903'
    print(DWH_DB_PASSWORD)
    DWH_PORT = '5439'
    print(DWH_PORT)

    DWH_ENDPOINT='redshift-cluster-1.cwsxsegnhrug.us-east-1.redshift.amazonaws.com:5439/testdb'
    print(DWH_ENDPOINT)
    DWH_ROLE_ARN='ARN=arn:aws:iam::254797166788:role/myRedshiftRole'
    print(DWH_ROLE_ARN)

    conn = psycopg2.connect(user = DWH_DB_USER, password = DWH_DB_PASSWORD, host = 'redshift-cluster-1.cwsxsegnhrug.us-east-1.redshift.amazonaws.com', port = DWH_PORT,dbname = DWH_DB)
    cur = conn.cursor()
    cur.execute(staging_songs_table_create)
    cur.execute(staging_songs_copy)
    conn.commit()
