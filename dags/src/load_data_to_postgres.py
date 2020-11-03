import psycopg2 as pgsql
import os

connection = "dbname=airflow port = 5432 user=airflow password=airflow host=postgres"
# connection = "dbname=test3 port = 5432 user=postgres password=abhishek host=localhost"

def load_data_to_postgres(file_path:str, create:str) -> None:
    """
    Loads data to postgresSQL.
    :param file_path: path
    :param create: SQL query
    :return: None
    """
    conn = pgsql.connect(connection)
    curr = conn.cursor()
    curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
    path_ = "cleaned_datasets/"+file_path
    for filename in os.listdir(path_):
        path = path_+"/" + str(filename)
        split_ = str(filename).split(".")
        name = "covid." + split_[0]
        drop = "DROP TABLE IF EXISTS " + name
        curr.execute(str(drop))
        create = "CREATE TABLE "+name+create
        curr.execute(str(create))
        with open(path, 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, name, sep=',')
    conn.commit()
    curr.close()
    conn.close()


class LoadData:

    def load_weekly_data(ds, **kwargs)->None:
        """
        Loads weekly data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(end_week date,start_week date,state text,covid_deaths integer,pneumonia_covid_deaths integer)"
        file_path = "weekly_data"
        load_data_to_postgres(file_path, create)

    def load_probability_of_new_cases_data(ds, **kwargs)->None:
        """
        Loads probability data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(submission_date date,state text,new_case integer,pnew_case integer,new_death integer,prob_death integer)"
        file_path = "probability_of_new_cases_data"
        load_data_to_postgres(file_path, create)

    def load_county_data(ds, **kwargs)->None:
        """
        Loads county data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(fips_code integer,covid_deaths integer)"
        file_path = "county_data"
        load_data_to_postgres(file_path, create)

    def load_age_and_sex_data(ds, **kwargs)->None:
        """
        Loads age and sex data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        file_path = "age_and_sex_data"
        create = "(state text,sex text,age_groups text,covid_deaths integer,pneumonia_covid_deaths integer)"
        load_data_to_postgres(file_path, create)

    def load_place_of_death(ds, **kwargs)->None:
        """
        Loads place of death data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        file_path = "place_of_death"
        create = "(state text,place_of_death text,covid_deaths integer,pneumonia_covid_deaths integer)"
        load_data_to_postgres(file_path, create)

    def load_race_data(ds, **kwargs)->None:
        """
        Loads race data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        file_path = "age_and_sex_data"
        create = "(state text,age_groups text,race text,covid_deaths integer,pneumonia_covid_deaths integer)"
        load_data_to_postgres(file_path, create)

    def race(ds, **kwargs)->None:
        """
        Loads different race type into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.race")
        curr.execute("""CREATE TABLE covid.race(
            race text)""")
        with open('cleaned_datasets/dependencies/race.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.race', sep=',')
        conn.commit()
        curr.close()
        conn.close()

    def start(ds, **kwargs)->None:
        """
        Loads start date into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.start")
        curr.execute("""CREATE TABLE covid.start(
            start_date date)""")
        with open('cleaned_datasets/dependencies/start.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.start', sep=',')
        conn.commit()
        curr.close()
        conn.close()

    def end(ds, **kwargs)->None:
        """
        Loads end date into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.end")
        curr.execute("""CREATE TABLE covid.end(
            end_date date)
            """)
        with open('cleaned_datasets/dependencies/end.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.end', sep=',')
        conn.commit()
        curr.close()
        conn.close()

    def place_of_death_postgres(ds, **kwargs)->None:
        """
        Loads unique place of deaths into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.place_of_death")
        curr.execute("""CREATE TABLE covid.place_of_death(
            place_of_death text)""")
        with open('cleaned_datasets/dependencies/place_of_death.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.place_of_death')
        conn.commit()
        curr.close()
        conn.close()

    def sex(ds, **kwargs)->None:
        """
        Loads unique sex listed into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.sex")
        curr.execute("""CREATE TABLE covid.sex(
            sex text)""")
        with open('cleaned_datasets/dependencies/sex.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.sex', sep=',')
        conn.commit()
        curr.close()
        conn.close()

    def age_groups(ds, **kwargs)->None:
        """
        Loads unique age groups into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.age_groups")
        curr.execute("""CREATE TABLE covid.age_groups(
            age_groups text)""")
        with open('cleaned_datasets/dependencies/age_groups.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.age_groups', sep=',')
        conn.commit()
        curr.close()
        conn.close()

    def states(ds, **kwargs)->None:
        """
        Loads unique states into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        conn = pgsql.connect(connection)
        curr = conn.cursor()
        curr.execute("CREATE SCHEMA IF NOT EXISTS covid")
        curr.execute("DROP TABLE IF EXISTS covid.states")
        curr.execute("""CREATE TABLE covid.states(
            fips_code integer,
            county text,
            state text,
            full_form text)
            """)
        with open('cleaned_datasets/dependencies/states.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            curr.copy_from(f1, 'covid.states', sep=',')
        conn.commit()
        curr.close()
        conn.close()



# ld = LoadData()
# ld.county_data()
# ld.probability_of_new_cases_data()
# ld.load_age_and_sex_data()
# ld.load_weekly_data()
# ld.load_place_of_death()
# ld.load_race_data()
# ld.race()
# ld.states()
# ld.start()
# ld.end()
# ld.sex()
# ld.place_of_death()
# ld.age_groups()