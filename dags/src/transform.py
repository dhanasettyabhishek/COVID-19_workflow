# Libraries
import pandas as pd
from datetime import timedelta
import os

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

    def read_file1(ds, **kwargs) -> None:
        path = 'dataFiles/ProvisionalCOVID-19DeathCountsbyWeekEndingDateandState.csv'
        try:
            data1 = pd.read_csv(path, delimiter=',', header=None, skiprows=1, names=['Data_as_of', 'Start_week', 'End_Week', 'Group', 'State', 'Indicator', 'COVID-19_Deaths', 'Total_Deaths', 'Percent_of_Expected_Deaths', 'Pneumonia Deaths', 'Pneumonia_and_COVID-19_Deaths', 'Influenza_Deaths', 'Pneumonia_Influenza_or_COVID-19_Deaths', 'Footnote'])
        except IOError:
            print('Could not read ' + path)
        truncated_data1 = data1.loc[:,['End_Week', 'State', 'COVID-19_Deaths', 'Pneumonia_and_COVID-19_Deaths']]
        truncated_data1.loc[:, 'End_Week'] = pd.to_datetime(truncated_data1.loc[:, 'End_Week'])
        truncated_data1['Start_Week'] = truncated_data1['End_Week'] - timedelta(7)
        truncated_data1[['COVID-19_Deaths', 'Pneumonia_and_COVID-19_Deaths']] = truncated_data1[['COVID-19_Deaths', 'Pneumonia_and_COVID-19_Deaths']].fillna(0)
        truncated_data1[['COVID-19_Deaths', 'Pneumonia_and_COVID-19_Deaths']] = truncated_data1[['COVID-19_Deaths', 'Pneumonia_and_COVID-19_Deaths']].astype('int')
        columns = list(truncated_data1.columns)
        columns.remove('Start_Week')
        columns.insert(1, 'Start_Week')
        truncated_data1 = truncated_data1[columns]
        print('Cleaning Data file 1 done.')
        try:
            os.mkdir("cleaned_datasets")
        except OSError:
            pass
        truncated_data1.to_csv("cleaned_datasets/truncated_data1.csv", index=False)
        print("Data copied")

    def read_file2(ds, **kwargs) -> None:
        path = 'dataFiles/UnitedStatesCOVID-19CasesandDeathsbyStateoverTime.csv'
        try:
            names = ['submission_date', 'state', 'tot_cases', 'conf_cases', 'prob_cases', 'new_case', 'pnew_case', 'tot_death', 'conf_death', 'prob_death', 'new_death', 'pnew_death', 'created_at', 'consent_cases', 'consent_deaths']
            data2 = pd.read_csv(path, delimiter=',', header=None, skiprows=1, names=names)
        except IOError:
            print('Could not read ' + path)
        truncated_data2 = data2.loc[:, ['submission_date', 'state', 'new_case', 'pnew_case', 'new_death', 'prob_death']]
        truncated_data2[['new_case', 'pnew_case', 'new_death', 'prob_death']] = truncated_data2[['new_case', 'pnew_case', 'new_death', 'prob_death']].fillna(0)
        truncated_data2[['new_case', 'pnew_case', 'new_death', 'prob_death']] = truncated_data2[['new_case', 'pnew_case', 'new_death', 'prob_death']].astype(int)
        print('Cleaning Data file 2 done.')
        try:
            os.mkdir("cleaned_datasets")
        except OSError:
            pass
        truncated_data2.to_csv("cleaned_datasets/truncated_data2.csv", index=False)
        print("Data copied")

    def read_file3(ds, **kwargs) -> None:
        path = 'dataFiles/ProvisionalCOVID-19DeathCountsintheUnitedStatesbyCounty.csv'
        try:
            data3 = pd.read_csv(path)
        except IOError:
            print('Could not read ' + path)
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

        truncated_data3 = data3.loc[:, ['FIPS County Code', 'Deaths involving COVID-19']]
        truncated_data3[['FIPS County Code', 'Deaths involving COVID-19']] = truncated_data3[
            ['FIPS County Code', 'Deaths involving COVID-19']].astype(int)

        print('Cleaning Data file 3 done.')
        try:
            os.mkdir("cleaned_datasets")
        except OSError:
            pass
        truncated_data3.to_csv("cleaned_datasets/truncated_data3.csv", index=False)
        states.to_csv("cleaned_datasets/states.csv", index=False)
        print("Data copied")

    def read_file4(ds, **kwargs) -> None:
        path = 'dataFiles/ProvisionalCOVID-19DeathCountsbySex,Age,andState.csv'
        try:
            data4 = pd.read_csv(path)
        except IOError:
            print('Could not read ' + path)

        truncated_data4 = data4.loc[:,
                          ['State', 'Sex', 'Age group', 'COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']]
        truncated_data4[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data4[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].fillna(0)
        truncated_data4[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data4[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].astype(int)

        sex = dict()
        sex['Sex'] = list(truncated_data4['Sex'].unique())
        sex = pd.DataFrame(sex, columns=['Sex'])

        age_groups = dict()
        age_groups['age_groups'] = list(truncated_data4['Age group'].unique())
        age_groups = pd.DataFrame(age_groups, columns=['age_groups'])
        print('Cleaning Data file 4 done.')
        try:
            os.mkdir("cleaned_datasets")
        except OSError:
            pass
        truncated_data4.to_csv("cleaned_datasets/truncated_data4.csv", index=False)
        age_groups.to_csv("cleaned_datasets/age_groups.csv", index=False)
        sex.to_csv("cleaned_datasets/sex.csv", index=False)
        print("Data copied")

    def read_file5(ds, **kwargs) -> None:
        path = 'dataFiles/ProvisionalCOVID-19DeathCountsbyPlaceofDeathandState.csv'
        try:
            data5 = pd.read_csv(path)
        except IOError:
            print('Could not read ' + path)

        truncated_data5 = data5.loc[:, ['State', 'Place of Death', 'COVID19 Deaths', 'Pneumonia and COVID19 Deaths']]
        truncated_data5[['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']] = truncated_data5[
            ['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']].fillna(0)
        truncated_data5[['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']] = truncated_data5[
            ['COVID19 Deaths', 'Pneumonia and COVID19 Deaths']].astype(int)

        place_of_death = dict()
        place_of_death['place_of_death'] = list(truncated_data5['Place of Death'].unique())
        place_of_death = pd.DataFrame(place_of_death, columns=['place_of_death'])

        start = dict()
        start['start'] = list(data5['Start week'].unique())
        start = pd.DataFrame(start, columns=['start'])

        end = dict()
        end['end'] = list(data5['End Week'].unique())
        end = pd.DataFrame(end, columns=['end'])
        print('Cleaning Data file 5 done.')
        try:
            os.mkdir("cleaned_datasets")
        except OSError:
            pass
        truncated_data5.to_csv("cleaned_datasets/truncated_data5.csv", index=False)
        place_of_death.to_csv("cleaned_datasets/place_of_death.csv", index=False)
        start.to_csv("cleaned_datasets/start.csv", index=False)
        end.to_csv("cleaned_datasets/end.csv", index=False)
        print("Data copied")

    def read_file6(ds, **kwargs) -> None:
        path = 'dataFiles/Deathsinvolvingcoronavirusdisease2019(COVID-19)byraceandHispanicorigingroupandage,bystate.csv'
        try:
            data6 = pd.read_csv(path)
        except IOError:
            print('Could not read ' + path)

        truncated_data6 = data6.loc[:, ['State', 'Age group', 'Race and Hispanic Origin Group', 'COVID-19 Deaths',
                                        'Pneumonia and COVID-19 Deaths']]
        truncated_data6[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data6[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].fillna(0)
        truncated_data6[['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']] = truncated_data6[
            ['COVID-19 Deaths', 'Pneumonia and COVID-19 Deaths']].astype(int)

        race = dict()
        race['race'] = list(truncated_data6['Race and Hispanic Origin Group'].unique())
        race = pd.DataFrame(race, columns=['race'])
        print('Cleaning Data file 6 done.')
        try:
            os.mkdir("cleaned_datasets")
        except OSError:
            pass
        truncated_data6.to_csv("cleaned_datasets/truncated_data6.csv", index=False)
        race.to_csv("cleaned_datasets/race.csv", index=False)
        print("Data copied")

# td = TransformData()
# td.read_file1()
# td.read_file2()
# td.read_file3()
# td.read_file4()
# td.read_file5()
# td.read_file6()
