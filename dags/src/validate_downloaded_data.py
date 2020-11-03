import os
import shutil
import pandas as pd

race = {"race_and_hispanic_origin_group", "age_group"}
place_of_death = {"place_of_death"}
sex_age = {"age_group", "sex"}
week_ending = {"indicator"}
county_data = {"fips_county_code"}
probability_of_new_cases = {"pnew_case", "pnew_death"}


def copying_data_to_downloaded_data(file_path, folder_name, idx) -> None:
    """
    Copying data to a downloaded data folder.
    :param file_path: Path
    :param folder_name: Folder name
    :param idx: count
    :return: None
    """
    path = "downloadedData/" + folder_name
    try:
        os.makedirs(path)
    except OSError:
        pass
    shutil.copy(file_path,
                path + "/" + folder_name + "_file_" + str(idx) + ".csv")
    idx += 1


class Validations:

    def validate_downloaded_data(ds, **kwargs)->None:
        """
        Validates downloaded data
        :param kwargs: Keyword argument
        :return: None
        """
        for filename in os.listdir("dataFiles"):
            filename = "dataFiles/" + filename
            data = pd.read_csv(filename, nrows=0)
            data = list(map(lambda x: x.replace(" ", "_"), list(data)))
            columns = set(map(lambda x: x.lower(), data))
            race_count, place_of_death_count, age_sex_count, weekly_count, county_count, probability_of_new_cases_count = 1, 1, 1, 1, 1, 1
            if len(columns.intersection(race)) == 2:
                copying_data_to_downloaded_data(filename, "race_data", race_count)
            elif len(columns.intersection(place_of_death)) == 1:
                copying_data_to_downloaded_data(filename, "place_of_death", place_of_death_count)
            elif len(columns.intersection(sex_age)) == 2:
                copying_data_to_downloaded_data(filename, "age_and_sex_data", age_sex_count)
            elif len(columns.intersection(week_ending)) == 1:
                copying_data_to_downloaded_data(filename, "weekly_data", weekly_count)
            elif len(columns.intersection(county_data)) == 1:
                copying_data_to_downloaded_data(filename, "county_data", county_count)
            elif len(columns.intersection(probability_of_new_cases)) == 2:
                copying_data_to_downloaded_data(filename, "probability_of_new_cases_data",probability_of_new_cases_count)

# vd = Validations()
# vd.validate_downloaded_data()