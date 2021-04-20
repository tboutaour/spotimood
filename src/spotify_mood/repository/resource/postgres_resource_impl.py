import psycopg2
from spotify_mood.repository.resource.postgres_resource import PostgresResource

class PostgresResourceImpl(PostgresResource):

    def __init__(self, host, db, user, password):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = "5432"
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
        df.write\
            .mode('append') \
            .option("driver", "org.postgresql.Driver")\
            .jdbc(f"jdbc:postgresql://{self.host}:{self.port}/{self.db}",
                  table,
                  properties={"user": self.user, "password": self.password})


    def update_many(self, table, updates, conditions):
        return self.__execute_query(f"""
                UPDATE {table} SET {updates}
                 WHERE {conditions}""", commit=True)

