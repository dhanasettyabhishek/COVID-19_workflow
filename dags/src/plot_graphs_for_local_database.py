import psycopg2 as pgsql
import pandas as pd
import plotly.express as px
import datetime
from datetime import datetime, timedelta

connection = "dbname=covid_workflow port = 5432 user=postgres password=abhishek host=localhost"


class PlotData:
    def plot_cases_recorded_yesterday(ds, **kwargs) -> None:
        """
        Reads data from postgresSQL.
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        yesterday = datetime.now() - timedelta(2)
        yesterday = yesterday.strftime('%Y-%m-%d')
        print(yesterday)
        query = "SELECT state, new_case FROM covid.probability_of_new_cases_data_1 WHERE submission_date = '" + yesterday + "'"
        curr.execute(query)
        rows = curr.fetchall()
        data = pd.DataFrame(rows)
        data.columns = [x.name for x in curr.description]
        fig = px.choropleth(locations=data['state'], locationmode="USA-states", color=data['new_case'],
                            scope="usa", title="New covid cases recorded on 11/01/2020")
        fig.show()
        conn.commit()
        curr.close()
        conn.close()


pd = PlotData()
pd.plot_cases_recorded_yesterday()
