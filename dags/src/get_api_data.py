# Libraries
import requests
import json
import os
from typing import Tuple, List


class GetData:

    def download_url(ds, **kwargs) -> Tuple[List, List]:
        """
        Filter's covid data from healthdata,
        and get the url links to download the
        filtered data along with the title

        :param kwargs: Keyword argument.
        :return: list download urls and their titles
        """
        url = "https://healthdata.gov/data.json"
        titles = []
        req = requests.get(url)
        data = json.loads(req.content.decode())
        covid = {'covid', 'covid-19', 'covid19'}
        state = {'state', 'states'}
        count = 0
        download_urls = []
        for i in data['dataset']:
            if i['@type'] == 'dcat:Dataset' and i['accessLevel'] == 'public':
                if 'distribution' in i:
                    title = i['title']
                    title = title.split(" ")
                    title = list(map(lambda x: x.lower(), title))
                    title = set(map(lambda x: x.replace("(", "").replace(")", "").replace(":", ""), title))
                    if len(title.intersection(covid)) >= 1 and len(title.intersection(state)) >= 1:
                        distribution = i['distribution']
                        for j in distribution:
                            if 'title' in j:
                                if j['title'] == 'csv':
                                    count += 1
                                    titles.append((count, i['title']))
                                    download_urls.append(j['downloadURL'])
        return download_urls, titles

    def download_data(**kwargs) -> None:
        """
        Downloads the data from the filtered data.
        :param kwargs: to get the values from the previous
        pipeline.
        :return: None
        """
        ti = kwargs['ti']
        download_urls, titles = ti.xcom_pull(task_ids='filter_covid_data')
        directory = "dataFiles"
        if not os.path.exists(directory):
            os.makedirs(directory)
        count_path = 0
        for i in download_urls:
            response = requests.get(i)
            name = str(titles[count_path][1])
            name = name.split(" ")
            name = "".join(name)
            filename = directory + "/" + name + ".csv"
            count_path += 1
            with open(filename, 'wb') as file:
                for line in response:
                    file.write(line)

        dir_path = os.path.dirname(os.path.realpath(__file__))
        cwd = os.getcwd()
        print(dir_path)
        print(cwd)
        print("========================> Downloaded " + str(count_path) + " files!")

# getData = GetData()
# urls, titles = getData.download_url()
# getData.download_data(urls, titles)
