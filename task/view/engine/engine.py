from bs4 import BeautifulSoup
import requests
import re
import csv
from contextlib import closing
from task.view.engine.data_object import Mean
import codecs
from stream_tools.kafka_ import producer



def compute(web_page, temp = False):
    links = []
    response = requests.get(web_page)
    soup_obj = BeautifulSoup(response.text, 'lxml')
    links_notprepared = soup_obj.find_all('a', href=re.compile('yellow_tripdata'))
    for link in links_notprepared:
        links.append(link.attrs['href'])
    if temp:
        temp = [re.search('\d{4}-\d{2}', str(x)).group() for x in temp]
        links = [link for link in links if re.search('\d{4}-\d{2}', link).group() in temp]
    return links


def do_the_rest(links, temp, window, streaming):
    obj = Mean(window)
    start_date = temp[0]
    end_date = temp[1]
    status = True
    for link in links:
        with closing(requests.get(link, stream=True)) as r:
            reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'))
            for row in reader:
                if row and ('trip_distance' not in row) and not streaming:
                    status = obj.temp_process_data(row, start_date, end_date, streaming)
                if row and ('trip_distance' not in row) and streaming:
                    status = obj.temp_process_data(row, start_date, end_date, streaming)
                    if type(status)!=type(True):
                        producer.send('streaming_data_forward', value={status[0]: status[1]})
                    else:
                        pass
                if not status:
                    return {"Window": obj.window,
                            "Days": (end_date-start_date).days,
                            "Start Date": start_date.date(),
                            "End Date": end_date.date(),
                            "Mean": obj.rolling_mean_results}
            producer.flush()

