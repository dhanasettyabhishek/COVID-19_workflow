import psycopg2 as pgsql
from airflow.models import Variable


class DatabaseConnection:
    def __init__(self):
        """
        Initialize database connection with a cursor.
        variables: 'connection' holds the connection information
                    'cur' holds the cursor information

        """
        self.connection = pgsql.connect("dbname={dbname} port = {port} user={user} password={password} host={host}"
                                        .format(dbname=Variable.get('dbname'), port=Variable.get('port'),
                                                user=Variable.get('user'), password=Variable.get('password'),
                                                host=Variable.get('host')))

        self.cur = self.connection.cursor()

    def query(self, query):
        """
        Executes the query passing in as an input.
        Args: 'query'
        """
        self.cur.execute(query)

    def close(self):
        """
        Commits the connection and closes the connection.
        """
        self.connection.commit()
        self.cur.close()
        self.connection.close()