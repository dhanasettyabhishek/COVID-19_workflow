# Libraries
import requests
import json
from typing import Tuple, List
import logging


class GetData:

    @staticmethod
    def filter_data(**kwargs) -> Tuple[List, List]:
        """
        Filter's covid data from health data database,
        and get the url links to download the
        filtered data along with the title

        :param kwargs: Keyword argument.
        :return: list of download urls and their titles
        """
        url = "https://healthdata.gov/data.json"
        titles = []
        req = requests.get(url)
        data = json.loads(req.content.decode())
        covid = {'covid', 'covid-19', 'covid19'}
        state = {'state', 'states'}
        count = 0
        download_urls = []
        for key in data['dataset']:
            if key['@type'] == 'dcat:Dataset' and key['accessLevel'] == 'public':
                try:
                    if 'distribution' in key:
                        title = key['title']
                        title = title.split(" ")
                        title = list(map(lambda x: x.lower(), title))
                        title = set(map(lambda x: x.replace("(", "").replace(")", "").replace(":", ""), title))
                        if len(title.intersection(covid)) >= 1 and len(title.intersection(state)) >= 1:
                            distribution = key['distribution']
                            for value in distribution:
                                try:
                                    if 'title' in value:
                                        if value['title'] == 'csv':
                                            count += 1
                                            titles.append((count, key['title']))
                                            download_urls.append(value['downloadURL'])
                                except KeyError:
                                    logging.warning("Current file doesn't have a title!")
                except KeyError:
                    logging.warning("URL to download the data is not present!")
        return download_urls, titles
