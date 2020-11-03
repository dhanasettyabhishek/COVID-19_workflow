import psycopg2 as pgsql

# connection = "dbname=airflow port = 5432 user=airflow password=airflow host=postgres"
connection = "dbname=covid_workflow port = 5432 user=postgres password=abhishek host=localhost"

def test_connection() -> None:
    """
    testing connection postgresSQL.
    """
    conn = pgsql.connect(connection)
    curr = conn.cursor()
    curr.execute("""
    SELECT * FROM covid.probability_of_new_cases_data_1
    WHERE submission_date = '2020-11-01'
    """)
    rows = curr.fetchall()
    assert len(rows) >= 0
    conn.commit()
    curr.close()
    conn.close()