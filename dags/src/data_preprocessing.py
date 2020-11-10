# Libraries
import pandas as pd
from datetime import timedelta
import os
import logging

us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands': 'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))


class DataPreprocessing:

    @staticmethod
    def weekly_data(**kwargs: dict) -> None:
        """
        If any weekly data is found,
        creates separate files in the weekly_data folder.
        :param kwargs: Keyword arguments
        :return: None
        """
        count = 0
        for filename in os.listdir("downloadedData/weekly_data"):
            count += 1
            path = "downloadedData/weekly_data/" + str(filename)
            try:
                data = "data" + str(count)
                data = pd.read_csv(path)
            except IOError:
                logging.warning('Could not read ' + path)
            truncated_data_name = "weekly_data_file_" + str(count)
            truncated_data = "truncated_data" + str(count)
            truncated_data = data.loc[:, ['End Week', 'State', 'COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']]
            truncated_data.loc[:, 'End Week'] = pd.to_datetime(truncated_data.loc[:, 'End Week'])
            truncated_data['Start Week'] = truncated_data['End Week'] - timedelta(7)
            truncated_data[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data[
                ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].fillna(0)
            truncated_data[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data[
                ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].astype('int')
            columns = list(truncated_data.columns)
            columns.remove('Start Week')
            columns.insert(1, 'Start Week')
            truncated_data = truncated_data[columns]
            logging.info('Cleaning Data file 1 done.')
            try:
                os.makedirs("cleaned_datasets/weekly_data")
            except OSError:
                logging.warning("Unable to create a directory - cleaned_datasets/weekly_data")
                pass
            path = "cleaned_datasets/weekly_data/" + truncated_data_name + ".csv"
            truncated_data.to_csv(path, index=False)
            logging.info("Data copied " + truncated_data_name)

    @staticmethod
    def probability_of_new_cases_data(**kwargs) -> None:
        """
        If the data contains, probability information,
        the data is stored in the 'probability_of_new_cases_data' folder.
        :param kwargs: Keyword arguments
        :return: None
        """
        count = 0
        for filename in os.listdir("downloadedData/probability_of_new_cases_data"):
            count += 1
            path = "downloadedData/probability_of_new_cases_data/" + str(filename)
            try:
                data = "data" + str(count)
                data = pd.read_csv(path)
            except IOError:
                logging.warning('Could not read ' + path)
            truncated_data_name = "probability_of_new_cases_data_" + str(count)
            truncated_data = "truncated_data" + str(count)
            truncated_data = data.loc[:,
                             ['submission_date', 'state', 'new_case', 'pnew_case', 'new_death', 'prob_death']]
            truncated_data[['new_case', 'pnew_case', 'new_death', 'prob_death']] = truncated_data[
                ['new_case', 'pnew_case', 'new_death', 'prob_death']].fillna(0)
            truncated_data[['new_case', 'pnew_case', 'new_death', 'prob_death']] = truncated_data[
                ['new_case', 'pnew_case', 'new_death', 'prob_death']].astype(int)
            logging.info('Cleaning Data file 2 done.')
            try:
                os.makedirs("cleaned_datasets/probability_of_new_cases_data")
            except OSError:
                logging.warning("Unable to create a directory - cleaned_datasets/weekly_data")
                pass
            path = "cleaned_datasets/probability_of_new_cases_data/" + truncated_data_name + ".csv"
            truncated_data.to_csv(path, index=False)
            logging.info("Data copied " + truncated_data_name)

    @staticmethod
    def county_data(**kwargs) -> None:
        """
        If the data contains, county information,
        the data is stored in the 'county_data' folder.
        Also, the dependency state information is
        added to the states folder
        :param kwargs: Keyword arguments
        :return: None
        """
        count = 0
        for filename in os.listdir("downloadedData/county_data"):
            count += 1
            path = "downloadedData/county_data/" + str(filename)
            try:
                data = "data" + str(count)
                data = pd.read_csv(path)
            except IOError:
                logging.warning('Could not read ' + path)

            fips_dict = dict()
            for i in data['FIPS County Code']:
                state = data[data['FIPS County Code'] == i]['State'].values[0]
                county_name = data[data['FIPS County Code'] == i]['County name'].values[0]
                if state in abbrev_us_state:
                    if i not in fips_dict:
                        fips_dict[i] = ([county_name, state, abbrev_us_state[state]])

            states = pd.DataFrame.from_dict(fips_dict, orient='index', columns=['County', 'State', 'Full Form'])
            states['FIPS code'] = states.index
            states.reset_index(drop=True, inplace=True)
            columns = list(states.columns)
            columns.remove('FIPS code')
            columns.insert(0, 'FIPS code')
            states = states[columns]
            truncated_data_name = "county_data_" + str(count)
            truncated_data = "truncated_data" + str(count)
            truncated_data = data.loc[:, ['FIPS County Code', 'Deaths involving COVID-19']]
            truncated_data[['FIPS County Code', 'Deaths involving COVID-19']] = truncated_data[
                ['FIPS County Code', 'Deaths involving COVID-19']].astype(int)
            logging.info('Cleaning Data file 3 done.')
            try:
                os.makedirs("cleaned_datasets/county_data")
            except OSError:
                logging.warning("Unable to create a directory - cleaned_datasets/county_data")
                pass
            try:
                os.makedirs("cleaned_datasets/dependencies")
            except OSError:
                logging.warning("Unable to create a directory - cleaned_datasets/dependencies")
                pass
            path = "cleaned_datasets/county_data/" + truncated_data_name + ".csv"
            truncated_data.to_csv(path, index=False)
            states.to_csv("cleaned_datasets/dependencies/states.csv", index=False)
            logging.info("Data copied " + truncated_data_name)

    @staticmethod
    def age_and_sex_data(**kwargs) -> None:
        """
        If the data contains age and sex information,
        the data is stored in the 'age_and_sex_data' folder.
        Also, the dependency information such as sex, age
        added to their respective folder.
        :param kwargs: Keyword arguments
        :return: None
        """
        count = 0
        for filename in os.listdir("downloadedData/age_and_sex_data"):
            count += 1
            path = "downloadedData/age_and_sex_data/" + str(filename)
            try:
                data = "data" + str(count)
                data = pd.read_csv(path)
            except IOError:
                logging.warning('Could not read ' + path)

        truncated_data_name = "age_and_sex_data_" + str(count)
        truncated_data = "truncated_data" + str(count)
        truncated_data = data.loc[:,
                         ['State', 'Sex', 'Age group', 'COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']]
        truncated_data[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].fillna(0)
        truncated_data[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].astype(int)

        sex = dict()
        sex['Sex'] = list(truncated_data['Sex'].unique())
        sex = pd.DataFrame(sex, columns=['Sex'])

        age_groups = dict()
        age_groups['age_groups'] = list(truncated_data['Age group'].unique())
        age_groups = pd.DataFrame(age_groups, columns=['age_groups'])
        logging.info('Cleaning Data file 4 done.')
        try:
            os.makedirs("cleaned_datasets/age_and_sex_data")
        except OSError:
            logging.warning("Unable to create a directory - cleaned_datasets/county_data")
            pass
        try:
            os.makedirs("cleaned_datasets/dependencies")
        except OSError:
            logging.warning("Unable to create a directory - cleaned_datasets/dependencies")
            pass
        path = "cleaned_datasets/age_and_sex_data/" + truncated_data_name + ".csv"
        truncated_data.to_csv(path, index=False)
        age_groups.to_csv("cleaned_datasets/dependencies/age_groups.csv", index=False)
        sex.to_csv("cleaned_datasets/dependencies/sex.csv", index=False)
        logging.info("Data copied " + truncated_data_name)

    @staticmethod
    def place_of_death(**kwargs) -> None:
        """
        If the data contains place of death information,
        the data is stored in the 'place_of_death' folder.
        Also, the dependency information such as place of death
        is added to its respective folder.
        :param kwargs: Keyword arguments
        :return: None
        """
        count = 0
        for filename in os.listdir("downloadedData/place_of_death"):
            count += 1
            path = "downloadedData/place_of_death/" + str(filename)
            try:
                data = "data" + str(count)
                data = pd.read_csv(path)
            except IOError:
                logging.warning('Could not read ' + path)
        truncated_data_name = "place_of_death_" + str(count)
        truncated_data = "truncated_data" + str(count)
        truncated_data = data.loc[:, ['State', 'Place of Death', 'COVID19 Deaths', 'Pneumonia and COVID19 Deaths']]
        truncated_data["Place of Death"] = truncated_data["Place of Death"].str.replace(',', '')
        truncated_data[['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']] = truncated_data[
            ['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']].fillna(0)
        truncated_data[['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']] = truncated_data[
            ['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']].astype(int)

        place_of_death = dict()
        place_of_death['place_of_death'] = list(truncated_data['Place of Death'].unique())
        place_of_death = pd.DataFrame(place_of_death, columns=['place_of_death'])

        start = dict()
        start['start'] = list(data['Start week'].unique())
        start = pd.DataFrame(start, columns=['start'])

        end = dict()
        end['end'] = list(data['End Week'].unique())
        end = pd.DataFrame(end, columns=['end'])
        logging.info('Cleaning Data file 5 done.')
        try:
            os.makedirs("cleaned_datasets/place_of_death")
        except OSError:
            logging.warning("Unable to create a directory - cleaned_datasets/place_of_death")
            pass
        try:
            os.makedirs("cleaned_datasets/dependencies")
        except OSError:
            logging.warning("Unable to create a directory - cleaned_datasets/dependencies")
            pass
        path = "cleaned_datasets/place_of_death/" + truncated_data_name + ".csv"
        truncated_data.to_csv(path, index=False)
        place_of_death.to_csv("cleaned_datasets/dependencies/place_of_death.csv", index=False)
        start.to_csv("cleaned_datasets/dependencies/start.csv", index=False)
        end.to_csv("cleaned_datasets/dependencies/end.csv", index=False)
        logging.info("Data copied " + truncated_data_name)

    @staticmethod
    def race_data(**kwargs) -> None:
        """
        If the data contains the race information,
        the data is stored in the 'race_data' folder.
        Also, the dependency information such as race
        is added to its respective folder.
        :param kwargs: Keyword arguments
        :return: None
        """
        count = 0
        for filename in os.listdir("downloadedData/race_data"):
            count += 1
            path = "downloadedData/race_data/" + str(filename)
            try:
                data = "data" + str(count)
                data = pd.read_csv(path)
            except IOError:
                logging.warning('Could not read ' + path)
        truncated_data_name = "race_data_" + str(count)
        truncated_data = "truncated_data" + str(count)
        truncated_data = data.loc[:, ['State', 'Age group', 'Race and Hispanic Origin Group', 'COVID-19 Deaths',
                                      'Pneumonia and COVID-19 Deaths']]
        truncated_data[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].fillna(0)
        truncated_data[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].astype(int)

        race = dict()
        race['race'] = list(truncated_data['Race and Hispanic Origin Group'].unique())
        race = pd.DataFrame(race, columns=['race'])
        logging.info('Cleaning Data file 6 done.')
        try:
            os.makedirs("cleaned_datasets/race_data")
        except OSError:
            logging.warning("Unable to create a directory - cleaned_datasets/race_data")
            pass
        try:
            os.makedirs("cleaned_datasets/dependencies")
        except OSError:
            logging.warning("Unable to create a directory - cleaned_datasets/dependencies")
            pass
        path = "cleaned_datasets/race_data/" + truncated_data_name + ".csv"
        truncated_data.to_csv(path, index=False)
        race.to_csv("cleaned_datasets/dependencies/race.csv", index=False)
        logging.info("Data copied " + truncated_data_name)