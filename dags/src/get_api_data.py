# Libraries
import requests
import json
import os


class GetData:

    def download_url(ds, **kwargs) -> []:
    # def download_url(self) -> []:
        url = "https://healthdata.gov/data.json"
        req = requests.get(url)
        data = json.loads(req.content.decode())
        covid = {'covid', 'covid-19', 'covid19'}
        state = {'state', 'states'}
        count = 0
        titles = []
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
        return download_urls

    def download_data(**kwargs) -> None:
    # def download_data(self, download_urls) -> None:
        ti = kwargs['ti']
        download_urls = ti.xcom_pull(task_ids='filter_covid_data')
        directory = "dataFiles"
        if not os.path.exists(directory):
            os.makedirs(directory)
        count_path = 0

        for i in download_urls:
            count_path += 1
            response = requests.get(i)
            filename = directory + "/" + "data" + str(count_path) + ".csv"
            with open(filename, 'wb') as file:
                for line in response:
                    file.write(line)

        print("========================> Downloaded " + str(count_path) + " files!")


# getData = GetData()
# urls = getData.download_url()
# getData.download_data(urls)
