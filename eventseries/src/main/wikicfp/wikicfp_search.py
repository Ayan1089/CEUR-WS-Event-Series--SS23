import json
import re
import time

import requests
from bs4 import BeautifulSoup

from wikicfp_events import *


def get_event(url):
    try:
        response_event = requests.get("http://www.wikicfp.com" + url)
        soup_event = BeautifulSoup(response_event.text, "html.parser")
        event_label = soup_event.title.get_text()
        event_wikicfp = event_cfp(event_label)
        start_times = soup_event.find_all("span", {"property": "v:startDate"})
        end_times = soup_event.find_all("span", {"property": "v:endDate"})
        for tim in start_times:
            start_date = tim["content"]
            event_wikicfp.set_startTime(start_date)
            break
        for tim in end_times:
            end_date = tim["content"]
            event_wikicfp.set_endTime(end_date)
            break
        print(event_label, start_date, end_date)
        response_event.close()

    except:
        response_event = requests.get("http://www.wikicfp.com" + url)
        soup_event = BeautifulSoup(response_event.text, "html.parser")
        event_label = soup_event.title.get_text()
        event_wikicfp = event_cfp(event_label)
        start_times = soup_event.find_all("span", {"property": "v:startDate"})
        end_times = soup_event.find_all("span", {"property": "v:endDate"})
        for time in start_times:
            start_date = time["content"]
            event_wikicfp.set_startTime(start_date)
            break
        for time in end_times:
            end_date = time["content"]
            event_wikicfp.set_endTime(end_date)
            break
        print(event_label, start_date, end_date)
        response_event.close()

    return event_wikicfp


class cfpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, event_cfp):
            # 将 Person 对象转换为字典
            return {
                "label": obj.label,
                "startTime": obj.startTime,
                "endTime": obj.endTime,
                "seriesLabel": obj.series,
                "series_dblp": obj.series_dblp,
            }

        return super().default(obj)


url = "http://www.wikicfp.com/cfp/series?t=c&i=A"
response = requests.get(url)
html_content = response.text
# print(html_content)
soup = BeautifulSoup(html_content, "html.parser")

# 根据网页结构找到包含会议系列的HTML元素，例如：
series_elements = soup.find_all("div", class_="contsec")
series_list = []
for series_element in series_elements:
    # 提取会议系列名称
    series_names = series_element.find_all("a")
    for link_element in series_names:
        href = link_element.get("href")
        if href.startswith("/cfp/series"):
            BtoZ_series = BeautifulSoup(
                requests.get("http://www.wikicfp.com" + href).text, "html.parser"
            ).find_all("div", class_="contsec")
            for BtoZ_series_element in BtoZ_series:
                # 提取会议系列名称
                BtoZ_series_names = BtoZ_series_element.find_all("a")
                for BtoZ_link_element in BtoZ_series_names:
                    href_bz = BtoZ_link_element.get("href")
                    if href_bz.startswith("/cfp/program"):
                        series_list.append("http://www.wikicfp.com" + href_bz)
                        print("http://www.wikicfp.com" + href_bz)
        if href.startswith("/cfp/program"):
            series_list.append("http://www.wikicfp.com" + href)
            # rep2 = requests.get('http://www.wikicfp.com'+href).text
            print("http://www.wikicfp.com" + href)
response.close()
print(len(series_list))
event_and_series = []
for series in series_list:
    response_series = requests.get(series)
    series_inhalt = BeautifulSoup(response_series.text, "html.parser")
    series_title = series_inhalt.title.get_text()
    series_title = re.sub(r"\b\d{4}\b|\.\.\.", "", series_title)
    series_wikicfp = series_cfp(label=series_title)
    events_elements = series_inhalt.find_all("div", class_="contsec")

    for event_element in events_elements:
        # print(event_element)
        event_names = event_element.find_all("a")

        for event_name in event_names:
            href = event_name.get("href")

            if href.startswith("http://dblp"):
                dblp_link = href
                series_wikicfp.set_dblplink(dblp_link)
                # print(dblp_link)
                # print(series_wikicfp.label)

            if href.startswith("/cfp/servlet/event.showcfp?eventid="):
                # print(BeautifulSoup(requests.get('http://www.wikicfp.com'+href).text, 'html.parser').find('title'))
                event_wikicfp = get_event(href)
                time.sleep(3)
                event_wikicfp.set_seriesLabel(series_wikicfp.label)
                event_wikicfp.set_dblplink(series_wikicfp.dblp)
                event_and_series.append(event_wikicfp)

            json_data = json.dumps(event_and_series, cls=cfpEncoder)
            # response_event.close()

        break

    response_series.close()

print(json_data)
