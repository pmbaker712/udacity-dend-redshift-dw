import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def decorate_console():
    print('\n=============================================\n')


def load_staging_tables(cur, conn):

    print('\nLoading Staging Tables...\n')
    
    for key in copy_table_queries.keys():
        try:
            cur.execute(copy_table_queries[key])
            conn.commit()
            print(key + ' Table Loaded.\n')
        except:
            print('An error occured.\n')

def insert_tables(cur, conn):
        
    print('\nLoading Final Tables...\n')
    
    for key in insert_table_queries.keys():
        try:
            cur.execute(insert_table_queries[key])
            conn.commit()
            print(key + ' Table Loaded.\n')
        except:
            print('An error occured.\n')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    decorate_console()
    
    load_staging_tables(cur, conn)
    print('\nStaging Tables Successfully Loaded.\n')
    decorate_console()

    insert_tables(cur, conn)
    print('\nFinal Tables Successfully Loaded.\n')
    decorate_console()
    
    print('COMPLETE')   
    decorate_console()


    conn.close()


if __name__ == "__main__":
    main()