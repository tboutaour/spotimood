from spotify_mood.repository.resource.postgres_resource import PostgresResource
from pyspark.sql import DataFrame
import psycopg2


class PostgresResourceImpl(PostgresResource):

    def __init__(self, host, db, user, password):
        self.conn = psycopg2.connect(
            host=host,
            database=db,
            port="5432",
            user=user,
            password=password)

    def __execute_query(self, query, commit=False):
        cursor = self.conn.cursor()
        cursor.execute(query)
        if commit:
            self.conn.commit()
        return cursor

    def get_many_from_table(self, table, fields, conditions):
        return self.__execute_query(f"""
                SELECT {fields} from {table} 
                WHERE {conditions}""").fetchmany()

    def get_one_from_table(self, table, fields, conditions):
        return self.__execute_query(f"""
                SELECT {fields} from {table} 
                WHERE {conditions}""").fetchone()

    def set_dataframe_to_table(self, df, table):
        """
        Using cursor.mogrify() to build the bulk insert query
        then cursor.execute() to execute the query
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        cursor = self.conn.cursor()
        values = [cursor.mogrify("(%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)

        try:
            cursor.execute(query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        print("execute_mogrify() done")
        cursor.close()

    def update_many(self, table, updates, conditions):
        return self.__execute_query(f"""
                UPDATE {table} SET {updates}
                 WHERE {conditions}""", commit=True)

