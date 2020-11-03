import os
from os import listdir


def test_datafiles():
    files = set()
    for file in listdir("../"):
        files.add(file)
    assert 'dataFiles' in files


def test_number_of_files():
    files = set()
    len_ = len(listdir("../dataFiles"))
    assert len_ > 1

