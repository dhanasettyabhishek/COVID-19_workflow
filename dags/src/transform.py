# Libraries
import pandas as pd
from datetime import timedelta

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


class TransformData:

    # def read_file1(self) -> pd.DataFrame:
    def read_file1(ds, **kwargs) -> pd.DataFrame:
        try:
            data1 = pd.read_csv("dataFiles/data1.csv")
        except IOError:
            print("Could not read data1.csv")
        truncated_data1 = data1.loc[:,
                          ["Data as of", "End Week", "State", "COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]]
        truncated_data1.loc[:, 'End Week'] = pd.to_datetime(truncated_data1.loc[:, 'End Week'], format="%m/%d/%Y")
        truncated_data1["Start Week"] = truncated_data1['End Week'] - timedelta(7)
        truncated_data1[["COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]] = truncated_data1[
            ["COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]].fillna(0)
        truncated_data1[["COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]] = truncated_data1[
            ["COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]].astype('int')
        columns = list(truncated_data1.columns)
        columns.remove('Start Week')
        columns.insert(1, 'Start Week')
        truncated_data1 = truncated_data1[columns]
        print("Cleaning Data file 1 done.")
        return truncated_data1

    # def read_file2(self) -> pd.DataFrame:
    def read_file2(ds, **kwargs) -> pd.DataFrame:
        try:
            data2 = pd.read_csv("dataFiles/data2.csv")
        except IOError:
            print("Could not read data2.csv")
        truncated_data2 = data2.loc[:, ["submission_date", "state", "new_case", "pnew_case", "new_death", "prob_death"]]
        truncated_data2[["new_case", "pnew_case", "new_death", "prob_death"]] = truncated_data2[
            ["new_case", "pnew_case", "new_death", "prob_death"]].fillna(0)
        truncated_data2[["new_case", "pnew_case", "new_death", "prob_death"]] = truncated_data2[
            ["new_case", "pnew_case", "new_death", "prob_death"]].astype(int)
        print("Cleaning Data file 2 done.")
        return truncated_data2

    # def read_file3(self) -> (pd.DataFrame, pd.DataFrame):
    def read_file3(ds, **kwargs) -> (pd.DataFrame, pd.DataFrame):
        try:
            data3 = pd.read_csv("dataFiles/data3.csv")
        except IOError:
            print("Could not read data3.csv")
        fips_dict = dict()

        for i in data3['FIPS County Code']:
            state = data3[data3['FIPS County Code'] == i]['State'].values[0]
            county_name = data3[data3['FIPS County Code'] == i]['County name'].values[0]
            if state in abbrev_us_state:
                if i not in fips_dict:
                    fips_dict[i] = ([county_name, state, abbrev_us_state[state]])

        states = df = pd.DataFrame.from_dict(fips_dict, orient='index', columns=['County', 'State', 'Full Form'])
        states['FIPS code'] = states.index
        states.reset_index(drop=True, inplace=True)
        columns = list(states.columns)
        columns.remove('FIPS code')
        columns.insert(0, 'FIPS code')
        states = states[columns]

        truncated_data3 = data3.loc[:, ["FIPS County Code", "Deaths involving COVID-19"]]
        truncated_data3[["FIPS County Code", "Deaths involving COVID-19"]] = truncated_data3[
            ["FIPS County Code", "Deaths involving COVID-19"]].astype(int)

        print("Cleaning Data file 3 done.")
        return states, truncated_data3
