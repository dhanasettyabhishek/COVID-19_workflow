from os import listdir

def test_cleaned_datasets():
    files = listdir("../cleaned_datasets")
    assert len(files) == 7

def test_age_and_sex_data():
    files = set()
    for file in listdir("../cleaned_datasets/age_and_sex_data"):
        files.add(file)
    assert len(files) > 0

def test_county_data():
    files = set()
    for file in listdir("../cleaned_datasets/county_data"):
        files.add(file)
    assert len(files) > 0

def test_place_of_death():
    files = set()
    for file in listdir("../cleaned_datasets/place_of_death"):
        files.add(file)
    assert len(files) > 0

def test_probability_data():
    files = set()
    for file in listdir("../cleaned_datasets/probability_of_new_cases_data"):
        files.add(file)
    assert len(files) > 0

def test_race_data():
    files = set()
    for file in listdir("../cleaned_datasets/race_data"):
        files.add(file)
    assert len(files) > 0

def test_weekly_data():
    files = listdir("../cleaned_datasets/weekly_data")
    assert len(files) > 0

def test_dependencies():
    files = listdir("../cleaned_datasets/dependencies")
    assert len(files) > 0