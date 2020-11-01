# Libraries
import pandas as pd
from datetime import timedelta


class TransformData1:

    def read_file(self) -> pd.DataFrame:
        try:
            data1 = pd.read_csv("dataFiles/data1.csv")
        except IOError:
            print("Could not read data1.csv")
        return data1

    def truncating_data1(self, data) -> pd.DataFrame:
        truncated_data1 = data.loc[:, ["Data as of", "End Week", "State", "COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]]
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
        return truncated_data1

td1 = TransformData1()
data1 = td1.read_file()
t_data1 = td1.truncating_data1(data1)
print(t_data1)