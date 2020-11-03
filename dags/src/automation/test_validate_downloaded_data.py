import os
from os import listdir

def test_downloadedData():
    files = set()
    for file in listdir("../downloadedData"):
        files.add(file)
    assert len(files) == 6

def test_age_and_sex_data():
    files = set()
    for file in listdir("../downloadedData/age_and_sex_data"):
        files.add(file)
    assert len(files) > 0

def test_county_data():
    files = set()
    for file in listdir("../downloadedData/county_data"):
        files.add(file)
    assert len(files) > 0

def test_place_of_death():
    files = set()
    for file in listdir("../downloadedData/place_of_death"):
        files.add(file)
    assert len(files) > 0

def test_probability_data():
    files = set()
    for file in listdir("../downloadedData/probability_of_new_cases_data"):
        files.add(file)
    assert len(files) > 0

def test_race_data():
    files = set()
    for file in listdir("../downloadedData/race_data"):
        files.add(file)
    assert len(files) > 0

def test_weekly_data():
    files = listdir("../downloadedData/weekly_data")
    assert len(files) > 0
