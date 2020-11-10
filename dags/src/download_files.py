import os
import requests
import logging


class DownloadData:

    def download_files(**kwargs) -> None:
        """
        Downloads files from the stored URL
        :param kwargs: Keyword arguments
        using XCom, able to access the return values of the previous task.
        :return: None
        """
        ti = kwargs['ti']
        download_urls, titles = ti.xcom_pull(task_ids='filter_covid_data')
        directory = "dataFiles"
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info("Created a Directory named 'dataFiles'")
        count_path = 0
        for url in download_urls:
            response = requests.get(url)
            name = str(titles[count_path][1])
            name = name.split(" ")
            name = "".join(name)
            filename = directory + "/" + name + ".csv"
            count_path += 1
            with open(filename, 'wb') as file:
                for line in response:
                    file.write(line)
            logging.info("Downloaded data from {url}".format(url=url))
        logging.info("========================> Downloaded " + str(count_path) + " files!")
