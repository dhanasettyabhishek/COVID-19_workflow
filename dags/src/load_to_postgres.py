import psycopg2 as pgsql

conn = pgsql.connect("dbname=covid_data port = 5432 user=postgres password=abhishek host=localhost")

curr = conn.cursor()
curr.execute("CREATE SCHEMA IF NOT EXISTS covid AUTHORIZATION postgres")

curr.execute("DROP TABLE IF EXISTS covid.truncated_data1")
curr.execute("""CREATE TABLE covid.truncated_data1(
    end_week date,
    start_week date,
    state text,
    covid_deaths integer,
    pneumonia_covid_deaths integer)
""")
with open('cleaned_datasets/truncated_data1.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.truncated_data1', sep=',')


curr.execute("DROP TABLE IF EXISTS covid.truncated_data2")
curr.execute("""CREATE TABLE covid.truncated_data2(
    submission_date date,
    state text,
    new_case integer,
    pnew_case integer,
    new_death integer,
    prob_death integer)
""")
with open('cleaned_datasets/truncated_data2.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.truncated_data2', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.states")
curr.execute("""CREATE TABLE covid.states(
    fips_code integer,
    county text,
    state text,
    full_form text)
    """)
with open('cleaned_datasets/states.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.states', sep=',')


curr.execute("DROP TABLE IF EXISTS covid.truncated_data3")
curr.execute("""CREATE TABLE covid.truncated_data3(
    fips_code integer,
    covid_deaths integer)
""")
with open('cleaned_datasets/truncated_data3.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.truncated_data3', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.sex")
curr.execute("""CREATE TABLE covid.sex(
    sex text)""")
with open('cleaned_datasets/sex.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.sex', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.age_groups")
curr.execute("""CREATE TABLE covid.age_groups(
    age_groups text)""")
with open('cleaned_datasets/age_groups.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.age_groups', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.truncated_data4")
curr.execute("""CREATE TABLE covid.truncated_data4(
    state text,
    sex text,
    age_groups text,
    covid_deaths integer,
    pneumonia_covid_deaths integer)
    """)
with open('cleaned_datasets/truncated_data4.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.truncated_data4', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.start")
curr.execute("""CREATE TABLE covid.start(
    start_date date)""")
with open('cleaned_datasets/start.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.start', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.end")
curr.execute("""CREATE TABLE covid.end(
    end_date date)
    """)
with open('cleaned_datasets/end.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.end', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.place_of_death")
curr.execute("""CREATE TABLE covid.place_of_death(
    place_of_death text)""")
with open('cleaned_datasets/place_of_death.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.place_of_death')

curr.execute("DROP TABLE IF EXISTS covid.truncated_data5")
curr.execute("""CREATE TABLE covid.truncated_data5(
    state text,
    place_of_death text,
    covid_deaths integer,
    pneumonia_covid_deaths integer)
    """)
with open('cleaned_datasets/truncated_data5.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.truncated_data5', sep=',')

curr.execute("DROP TABLE IF EXISTS covid.race")
curr.execute("""CREATE TABLE covid.race(
    race text)""")
with open('cleaned_datasets/race.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.race', sep=',')


curr.execute("DROP TABLE IF EXISTS covid.truncated_data6")
curr.execute("""CREATE TABLE covid.truncated_data6(
    state text,
    age_groups text,
    race text,
    covid_deaths integer,
    pneumonia_covid_deaths integer)
    """)
with open('cleaned_datasets/truncated_data6.csv', 'r') as f1:
    next(f1)  # Skip the header row.
    curr.copy_from(f1, 'covid.truncated_data6', sep=',')




conn.commit()
curr.close()
conn.close()
