import os
import requests
import json
from bs4 import BeautifulSoup
import datetime
from datetime import date,timedelta

agents= {"User-Agent": "Chrome/87.0.4280.88"}

def render_response(data, as_json=False):
    if as_json is True:
        return json.dumps(data)
    else:
        return data

def get_average_aum():
    """
       gets the Avearage AUM data for all Fund houses
       :param as_json: True / False
       :param year_quarter: string 'July - September 2020'
       #quarter format should like - 'April - June 2020'
       :return: json format
       :raises: HTTPError, URLError
   """
    all_funds_aum = []
    #url =  "https://www.amfiindia.com/research-information/aum-data/average-aum"
    url = "https://www.amfiindia.com/research-information/aum-data/average-aum/modules/AvergaeAUMYearByMFId"
    html = requests.post(url,headers=agents,data={"AUmType":'S',"AumCategoryType":"Categorywise","AumMFName":"All","AumYear":"April 2021 - March 2022","AumQuarter":"April - June 2021" })
    #html = requests.post(url, headers=agents, data={"AUmType": 'F', "Year_Quarter": 'April - June 2021'})
    print(html.text)
    #print(html)
    #html = requests.post(url,headers=agents,data={"AUmType":'S',"AumCategoryType":"Categorywise","AumMFName":"All","AumYear":"April 2021 - March 2022","AumQuarter":"April - June 2021" })
    soup = BeautifulSoup(html.text, 'html.parser')
    #print(soup)
    rows = soup.select("table tbody tr")
    for row in rows:
        aum_fund = {}
        if len(row.findAll('td')) > 1:
            aum_fund['Fund Name'] = row.select("td")[1].get_text().strip()
            aum_fund['AAUM Overseas'] = row.select("td")[2].get_text().strip()
            aum_fund['AAUM Domestic'] = row.select("td")[3].get_text().strip()
            all_funds_aum.append(aum_fund)
            aum_fund = None
    return render_response(all_funds_aum)


print(get_average_aum())