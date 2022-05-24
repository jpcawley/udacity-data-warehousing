import configparser
import psycopg2
import pandas as pd

tables = ['songplays', 'users', 'artists', 'songs', 'time']

table_count_queries = [
    'SELECT COUNT(*) FROM songplays', 'SELECT COUNT(*) FROM users', 'SELECT COUNT(*) FROM artists', 'SELECT COUNT(*) FROM songs', 'SELECT COUNT(*) FROM time']

table_prev_queries = [
    'SELECT * FROM songplays LIMIT 5', 'SELECT * FROM users LIMIT 5', 'SELECT * FROM artists LIMIT 5', 'SELECT * FROM songs LIMIT 5', 'SELECT * FROM time LIMIT 5']

# ADD ADDITIONAL VALIDATION QUERIES HERE (OPTIONAL)
analysis_queries = []

def get_table_counts(cur):
    '''Gets the row count for each table in the postgres database.
        parameters:
            cur: cursor object for query execution
    '''
    for query, table in zip(table_count_queries, tables):
        print(query)
        cur.execute(query)
        count, = cur.fetchone()
        print('{} rows\n'.format(count))

def preview(cur, conn):
    '''Prints a preview of each table in the postgres database.
        parameters:
            cur: cursor object for query execution
            conn: database connection object
    '''
    for query, table in zip(table_prev_queries, tables):
        try:
            q = pd.read_sql_query(query, conn)
            df = pd.DataFrame(q)
            pd.set_option('display.max_columns', None)
            print('First 5 rows of the {} table.\n'.format(table))
            print(df)
        except Exception as e:
            print(e)
            
def analyze(cur, conn):
    '''Prints the results of each analytical query written in analysis_queries.
        parameters:
            cur: cursor object for query execution
            conn: database connection object
    '''
    if len(analysis_queries) == 0:
        print('No queries yet exist. Please exit and add queries to validate.py.\n')
    else:
        for query in analysis_queries:
            cur.execute(query)
            results = cur.fetchall()

            for row in results:
                print(row)

def main():
    '''Main method to run all methods and create the database connection.'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    while True:
        try:
            query_type = int(input('Please enter 1 to get table counts, 2 to see a preview of the tables, or 3 to create your own queries: '))
            
            if query_type == 1:
                get_table_counts(cur)
            elif query_type == 2:
                preview(cur, conn)
            else:
                analyze(cur, conn)
            
            try:
                cont = str(input('Would you like to execute more queries? (Y/N): \n')).upper()
            
                if cont != 'Y':
                    break
            except ValueError:
                print('Invalid input. Please enter Y or N.')
        except ValueError:
            print('Invalid input. Please input an integer either 1, 2, or 3.')
    
    print('Closing database connection; cleaning up AWS resource.\n')
    conn.close()


if __name__ == "__main__":
    main()