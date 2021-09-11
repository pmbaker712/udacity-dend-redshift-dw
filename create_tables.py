import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def decorate_console():
    print('\n=============================================\n')

def drop_tables(cur, conn):
    
    print('\nDropping Tables...\n')
    
    for key in drop_table_queries.keys():
        try:
            cur.execute(drop_table_queries[key])
            conn.commit()
            print(key + ' Table Dropped.\n')
        except:
            print('An error occured.\n')


def create_tables(cur, conn):
    
    print('\nCreating Tables...\n')
    
    for key in create_table_queries.keys():
        try:
            cur.execute(create_table_queries[key])
            conn.commit()
            print(key + ' Table Created.\n')
        except:
            print('An error occured.\n')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    decorate_console()

    drop_tables(cur, conn)
    
    print('\nTables Successfully Dropped.\n')
    decorate_console()
  
    create_tables(cur, conn)
    
    print('\nTables Successfully Created.\n')
    decorate_console()
   
    print('COMPLETE')
    print('\nNow run python etl.py') 
    decorate_console()


    conn.close()


if __name__ == "__main__":
    main()