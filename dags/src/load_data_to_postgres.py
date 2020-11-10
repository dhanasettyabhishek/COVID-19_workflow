import psycopg2 as pgsql
import os
from airflow.models import Variable
from src.postgres_connection import DatabaseConnection

# connection = "dbname={dbname} port = {port} user={user} password={password} host={host}".format(
#     dbname=Variable.get('dbname'), port=Variable.get('port'), user=Variable.get('user'),
#     password=Variable.get('password'), host=Variable.get('host'))


def load_data_to_postgres(file_path: str, create: str) -> None:
    """
    Loads data to postgresSQL.
    :param file_path: path
    :param create: SQL query
    :return: None
    """
    db = DatabaseConnection()
    db.cur.execute("CREATE SCHEMA IF NOT EXISTS covid")
    path_ = "cleaned_datasets/" + file_path
    for filename in os.listdir(path_):
        path = path_ + "/" + str(filename)
        split_ = str(filename).split(".")
        name = "covid." + split_[0]
        drop = "DROP TABLE IF EXISTS " + name
        db.query(str(drop))
        create = "CREATE TABLE " + name + create
        db.query(str(create))
        with open(path, 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, name, sep=',')
    db.close()


class LoadData:

    @staticmethod
    def load_weekly_data(**kwargs: dict) -> None:
        """
        Loads weekly data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(end_week date,start_week date,state text,covid_deaths integer,pneumonia_covid_deaths integer)"
        file_path = "weekly_data"
        load_data_to_postgres(file_path, create)

    @staticmethod
    def load_probability_of_new_cases_data(**kwargs: dict) -> None:
        """
        Loads probability information to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(submission_date date,state text,new_case integer,pnew_case integer,new_death integer,prob_death integer)"
        file_path = "probability_of_new_cases_data"
        load_data_to_postgres(file_path, create)

    @staticmethod
    def load_county_data(**kwargs: dict) -> None:
        """
        Loads county data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(fips_code integer,covid_deaths integer)"
        file_path = "county_data"
        load_data_to_postgres(file_path, create)

    @staticmethod
    def load_age_and_sex_data(**kwargs: dict) -> None:
        """
        Loads age and sex data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        file_path = "age_and_sex_data"
        create = "(state text,sex text,age_groups text,covid_deaths integer,pneumonia_covid_deaths integer)"
        load_data_to_postgres(file_path, create)

    @staticmethod
    def load_place_of_death(**kwargs: dict) -> None:
        """
        Loads place of death information to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(state text,place_of_death text,covid_deaths integer,pneumonia_covid_deaths integer)"
        file_path = "race_data"
        load_data_to_postgres(file_path, create)

    @staticmethod
    def load_race_data(**kwargs: dict) -> None:
        """
        Loads race data to PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        create = "(state text,age_groups text,race text,covid_deaths integer,pneumonia_covid_deaths integer)"
        file_path = "age_and_sex_data"
        load_data_to_postgres(file_path, create)

    @staticmethod
    def race(**kwargs: dict) -> None:
        """
        Loads different race type information into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.race")
        db.query("""CREATE TABLE covid.race(race text)""")
        with open('cleaned_datasets/dependencies/race.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.race', sep=',')
        db.close()

    @staticmethod
    def start_week(**kwargs: dict) -> None:
        """
        Loads start date into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.start")
        db.query("""CREATE TABLE covid.start(start_date date)""")
        with open('cleaned_datasets/dependencies/start.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.start', sep=',')
        db.close()

    @staticmethod
    def end_week(**kwargs: dict) -> None:
        """
        Loads end date into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.end")
        db.query("""CREATE TABLE covid.end(end_date date)""")
        with open('cleaned_datasets/dependencies/end.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.end', sep=',')
        db.close()

    @staticmethod
    def unique_place_of_death(**kwargs: dict) -> None:
        """
        Loads unique place of deaths into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.place_of_death")
        db.query("""CREATE TABLE covid.place_of_death(place_of_death text)""")
        with open('cleaned_datasets/dependencies/place_of_death.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.place_of_death')
        db.close()

    @staticmethod
    def sex(**kwargs) -> None:
        """
        Loads unique sex listed into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.sex")
        db.query("""CREATE TABLE covid.sex(sex text)""")
        with open('cleaned_datasets/dependencies/sex.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.sex', sep=',')
        db.close()

    @staticmethod
    def age_groups(**kwargs) -> None:
        """
        Loads unique age groups into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.age_groups")
        db.query("""CREATE TABLE covid.age_groups(age_groups text)""")
        with open('cleaned_datasets/dependencies/age_groups.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.age_groups', sep=',')
        db.close()

    @staticmethod
    def states(**kwargs) -> None:
        """
        Loads unique states into PostgresSQL.
        :param kwargs: keyword argument
        :return: None
        """
        db = DatabaseConnection()
        db.query("CREATE SCHEMA IF NOT EXISTS covid")
        db.query("DROP TABLE IF EXISTS covid.states")
        db.query("""CREATE TABLE covid.states(
            fips_code integer,
            county text,
            state text,
            full_form text)
            """)
        with open('cleaned_datasets/dependencies/states.csv', 'r') as f1:
            next(f1)  # Skip the header row.
            db.cur.copy_from(f1, 'covid.states', sep=',')
        db.close()