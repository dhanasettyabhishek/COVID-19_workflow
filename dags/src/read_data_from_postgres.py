import psycopg2 as pgsql
import pandas as pd
import datetime
from datetime import datetime, timedelta
from airflow.models import Variable

connection = "dbname={dbname} port = {port} user={user} password={password} host={host}".format(
    dbname=Variable.get('dbname'), port=Variable.get('port'), user=Variable.get('user'),
    password=Variable.get('password'), host=Variable.get('host'))

class ReadData:

    @staticmethod
    def read_data_from_postgres(**kwargs: dict) -> pd.DataFrame:
        """
        Reads data from postgresSQL.
        params: keyword arguments
        return: Dataframe
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        yesterday = datetime.now() - timedelta(2)
        yesterday = yesterday.strftime('%Y-%m-%d')
        print(yesterday)
        query = "SELECT state, new_case FROM covid.probability_of_new_cases_data_1 WHERE submission_date = '" \
                + yesterday + "'"
        curr.execute(query)
        rows = curr.fetchall()
        data = pd.DataFrame(rows)
        data.columns = [x.name for x in curr.description]
        conn.commit()
        curr.close()
        conn.close()
        return data
