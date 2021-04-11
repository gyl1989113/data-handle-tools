# -*- coding: utf-8 -*-
import json

import requests

# api url
from retrying import retry

URL = "http://api.map.baidu.com/geocoder/v2/"
# api key
AK = "GnZYZ8IczNld5GWIzzGdaz2Qjc32R3DP"
# 输出方式
OUTPUT = "json"


@retry(stop_max_attempt_number=5)
def get_lat_lng(address):
    """
    根据地址获取经纬度
    :param address: 地址
    :return:
    """
    url = URL + "?" + "address=" + address + "&output=" + OUTPUT + "&ak=" + AK
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_area(lat_lng):
    """
    根据经纬度获取地址
    :param lat_lng: 经纬度
    :return:
    """
    url = URL + "?location={}&output={}&pois=1&latest_admin=1&ak={}".format(lat_lng, OUTPUT, AK)
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_periphery(classify, tag, lat_lng, radius, page_num, coord_type=3):
    """
    根据经纬度获取周边
    :param classify: (银行)
    :param tag: (金融)
    :param lat_lng: 经纬度
    :param radius: 半径(米)
    :param page_num: 页数
    :param coord_type: 坐标类型
    :return:
    """
    #  &coord_type=1
    url = f"http://api.map.baidu.com/place/v2/search?" \
        f"query={classify}&tag={tag}&location={lat_lng}&page_size=20&radius={radius}&" \
        f"output=json&ak={AK}&page_num={page_num}&coord_type={coord_type}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_infomation(uid):
    url = f"http://api.map.baidu.com/place/v2/detail?uid={uid}&output=json&scope=2&ak={AK}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


@retry(stop_max_attempt_number=5)
def get_infomation(query, city):
    url = f"http://api.map.baidu.com/place/v2/suggestion?query={query}&region={city}&city_limit=true&output=json&ak={AK}"
    response = requests.get(url)
    temp = json.loads(response.content)
    response.close()
    return temp


if __name__ == '__main__':
    # print(get_lat_lng("中国"))
    # print(get_area("37.5503394745908,104.11412925347894"))
    # print(get_lat_lng("上海市迪亚天天NO.5297"))
    # print(get_area("31.235929042252014,121.48053886017651"))
    # print(get_lat_lng("迪亚天天(图们店)"))
    # print(get_lat_lng("上海嘉定区塔城东路226"))
    # print(get_area("42.97428349907469,129.85038151374656"))
    # print(get_lat_lng("上海崇明区长兴岛壹街区金滂路好又多超市旁"))
    # print(get_area("31.268240662398377,121.49585681873268"))
    # print(get_lat_lng("上海崇明区长兴岛壹街区金滂路"))
    # print(get_area("31.62856998440405,121.40355686271847"))
    # print(get_lat_lng("上海市崇明区星村公路2109临"))
    # print(get_area("31.62856998440405,121.40355686271847"))
    # print(get_lat_lng("上海市迪亚天天图们路38-2临"))
    # print(get_lat_lng("粤澳合作产业园粤澳中医药科技产业园"))

    # print(get_lat_lng("徐汇区"))

    # print(get_area("39.955089884066034,116.482485906868"))

    # print(get_lat_lng("北京安定镇于家务公交车站"))
    #
    # print(get_area("31.34318640790373, 118.37659674943518"))

    # quit()
    # # 获取周边
    station = "安定镇于家务"
    i = 0
    while True:
        x3 = get_periphery(classify="公交车站", tag="交通设施", lat_lng="39.62069758500963,116.56424983967442", radius=3000,
                           page_num=i)
        # print(x3)
        for nearby in x3["results"]:
            if station in nearby["name"]:
                print(nearby)
                break
        i += 1
        if len(x3["results"]) != 20:
            break
