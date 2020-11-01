# Libraries
import pandas as pd
from datetime import timedelta


class TransformData:

    # def read_file1(self) -> pd.DataFrame:
    def read_file1(ds, **kwargs) -> pd.DataFrame:
        try:
            data1 = pd.read_csv("dataFiles/data1.csv")
        except IOError:
            print("Could not read data1.csv")
        truncated_data1 = data1.loc[:, ["Data as of", "End Week", "State", "COVID-19 Deaths", "Pneumonia and COVID-19 Deaths"]]
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
