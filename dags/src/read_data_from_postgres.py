import pandas as pd
import datetime
from datetime import datetime, timedelta
from src.postgres_connection import DatabaseConnection
import logging


class ReadData:

    @staticmethod
    def read_data_from_postgres(**kwargs: dict) -> pd.DataFrame:
        """
        Reads data from postgresSQL.
        params: keyword arguments
        return: Dataframe
        """
        db = DatabaseConnection()
        yesterday = datetime.now() - timedelta(1)
        yesterday = yesterday.strftime('%Y-%m-%d')
        logging.info(yesterday)
        query = "SELECT state, new_case FROM covid.probability_of_new_cases_data_1 WHERE submission_date = '" \
                + yesterday + "'"
        db.query(query)
        rows = db.cur.fetchall()
        data = pd.DataFrame(rows)
        data.columns = [x.name for x in db.cur.description]
        db.close()
        return data
