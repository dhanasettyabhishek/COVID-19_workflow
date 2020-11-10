import os
import shutil
import pandas as pd
import logging

race = {"race_and_hispanic_origin_group", "age_group"}
place_of_death = {"place_of_death"}
sex_age = {"age_group", "sex"}
week_ending = {"indicator"}
county_data = {"fips_county_code"}
probability_of_new_cases = {"pnew_case", "pnew_death"}


def copying_data_to_downloaded_data(file_path: str, folder_name: str, idx: int) -> None:
    """
    Copying data to a downloaded data folder.
    :param file_path: Path to copy the data from
    :param folder_name: Folder name of the validated data.
    :param idx: index is updated if multiple files are downloaded.
    :return: None
    """
    path = "downloadedData/" + folder_name
    try:
        os.makedirs(path)
    except OSError:
        logging.info("Directory 'downloadedData' is already created or unable to created")
        pass
    shutil.copy(file_path,
                path + "/" + folder_name + "_file_" + str(idx) + ".csv")
    idx += 1


class Validations:

    @staticmethod
    def validate_downloaded_data(**kwargs: dict) -> None:
        """
        Validates downloaded data
        based on race, place of death, age, sex,
        county data, probability of new cases, and
        weekly data.
        :param kwargs: Keyword argument
        :return: None Validates the downloaded data.
        """
        for filename in os.listdir("dataFiles"):
            filename = "dataFiles/" + filename
            data = pd.read_csv(filename, nrows=0)
            data = list(map(lambda x: x.replace(" ", "_"), list(data)))
            columns = set(map(lambda x: x.lower(), data))
            race_count, place_of_death_count, age_sex_count, weekly_count, county_count, probability_of_new_cases_count\
                , other_count = 1, 1, 1, 1, 1, 1, 1
            if len(columns.intersection(race)) == 2:
                copying_data_to_downloaded_data(filename, "race_data", race_count)
                logging.info("Copied data from {} to 'race_data".format(filename))
            elif len(columns.intersection(place_of_death)) == 1:
                copying_data_to_downloaded_data(filename, "place_of_death", place_of_death_count)
                logging.info("Copied data from {} to 'place_of_death".format(filename))
            elif len(columns.intersection(sex_age)) == 2:
                copying_data_to_downloaded_data(filename, "age_and_sex_data", age_sex_count)
                logging.info("Copied data from {} to 'age_and_sex_data".format(filename))
            elif len(columns.intersection(week_ending)) == 1:
                copying_data_to_downloaded_data(filename, "weekly_data", weekly_count)
                logging.info("Copied data from {} to 'weekly_data".format(filename))
            elif len(columns.intersection(county_data)) == 1:
                copying_data_to_downloaded_data(filename, "county_data", county_count)
                logging.info("Copied data from {} to 'county_data".format(filename))
            elif len(columns.intersection(probability_of_new_cases)) == 2:
                copying_data_to_downloaded_data(filename, "probability_of_new_cases_data",
                                                probability_of_new_cases_count)
                logging.info("Copied data from {} to 'probability_of_new_cases_data".format(filename))
            else:
                copying_data_to_downloaded_data(filename, "other", other_count)
                logging.warning("Data in the folder 'other' needs to be preprocessed")
