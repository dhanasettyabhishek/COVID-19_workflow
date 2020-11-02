import psycopg2 as pgsql

conn = pgsql.connect("dbname=covid_data port = 5432 user=postgres password=abhishek host=localhost")

curr = conn.cursor()
curr.execute("CREATE SCHEMA IF NOT EXISTS covid AUTHORIZATION postgres")


class LoadData:

    def load_data1(ds, **kwargs):
        curr.execute("DROP TABLE IF EXISTS covid.truncated_data1")
        curr.execute("""CREATE TABLE covid.truncated_data1(
            end_week date,
            start_week date,
            state text,
            covid_deaths integer,
            pneumonia_and_covid_deaths integer)
        """)
        with open('cleaned_datasets/truncated_data1.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.truncated_data1', sep=',')


ld = LoadData()
ld.load_data1()
conn.commit()
curr.close()
conn.close()
