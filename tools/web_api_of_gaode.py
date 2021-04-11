# -*- coding: utf-8 -*-
import json

import requests

URL_FOR_LAT_LNG = "https://restapi.amap.com/v3/geocode/geo"
URL_FOR_AREA = "https://restapi.amap.com/v3/geocode/regeo"
AK = "09d8c58e44834029245dae4478dfbaec"
OUTPUT = "json"


def gaode_get_lat_lng(address):
    url = URL_FOR_LAT_LNG + "?" + "key=" + AK + "&address=" + address
    # url = url + "?location={}&output=json&pois=1&ak={}".format(address, ak)
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


def gaode_get_area(lat_lng):
    url = "https://restapi.amap.com/v3/geocode/regeo"
    url = URL_FOR_AREA + "?" + "key=" + AK + "&location=" + lat_lng
    # url = url + "?location={}&output=json&pois=1&ak={}".format(address, ak)
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


if __name__ == '__main__':
    temp = gaode_get_lat_lng("宿迁市")
    print(temp)
