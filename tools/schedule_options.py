# coding=utf-8
import json
import time
import requests


def read_txt(path):
    with open(path, 'r') as f:
        content = f.read().splitlines()
    result = []
    for con in content:
        if con.strip() != '':
            result.append(con)
    return result


class schedule:
    def __init__(self, url_title, code, x_user_token):
        self.url_title = url_title
        self.code = code
        self.header = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,en-US;q=0.7,en;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Cookie": "last_page_name=home_index; locking=0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "x-menu-code": "sch.jobEntity",
            "x-tenant-required": "false"
        }
        self.header['x-user-token'] = x_user_token
        self.header['Referer'] = self.url_title

    def schedule_change(self, mode):
        list_of_details = self.schedule_status()
        if mode == 'start':
            for item in list_of_details:
                # 任务未开始
                if item['status'] != 'EXECUTING':
                    url = self.url_title + 'api/sch/jobEntity/start/' + item['code']
                    res = requests.post(url=url, headers=self.header)
                    if res.status_code == 200:
                        status = self.schedule_status_single(item['code'])['status']
                        print(item['code'], "操作成功,当前的状态为：", status)
                else:
                    print(item['code'], "已处于开始状态")
        elif mode == 'pause':
            for item in list_of_details:
                # 执行状态可暂停
                if item['status'] == 'EXECUTING':
                    url = self.url_title + 'api/sch/jobEntity/pause/' + item['code']
                    res = requests.post(url=url, headers=self.header)
                    if res.status_code == 200:
                        status = self.schedule_status_single(item['code'])['status']
                        print(item['code'], "操作成功,当前的状态为：", status)
                elif item['status'] == 'PAUSED':
                    print(item['code'], "目前处于暂停状态")
                elif item['status'] == 'STOPPED':
                    print(item['code'], "目前处于终止状态")
        elif mode == 'stop':
            for item in list_of_details:
                # 停止已启动的
                if item['status'] != 'STOPPED':
                    url = self.url_title + 'api/sch/jobEntity/stop/' + item['code']
                    res = requests.post(url=url, headers=self.header)
                    if res.status_code == 200:
                        status = self.schedule_status_single(item['code'])['status']
                        print(item['code'], "操作成功,当前的状态为：", status)
                elif item['status'] == 'STOPPED':
                    print(item['code'], "已处于终止状态")

    def schedule_restart(self):
        self.schedule_change('stop')
        time.sleep(1)
        self.schedule_change('start')

    def schedule_status(self):
        url = self.url_title + 'api/sch/jobEntity/query?pageSize=10&pageNumber=1&__LIKES_code=' + self.code
        response_of_query = requests.get(url=url, headers=self.header)
        list_of_detail = []
        if response_of_query.status_code == 200:
            response_json = json.loads(response_of_query.text)
            total = response_json['total']
            for i in range(total):
                dic = {}
                dic['code'] = response_json['rows'][i]['code']
                dic['name'] = response_json['rows'][i]['name']
                dic['status'] = response_json['rows'][i]['status']
                list_of_detail.append(dic)
        return list_of_detail

    def schedule_status_single(self, code):
        url = self.url_title + 'api/sch/jobEntity/query?pageSize=10&pageNumber=1&__LIKES_code=' + code
        response_of_query = requests.get(url=url, headers=self.header)
        status_single = {}
        if response_of_query.status_code == 200:
            response_json = json.loads(response_of_query.text)
            status_single['code'] = response_json['rows'][0]['code']
            status_single['name'] = response_json['rows'][0]['name']
            status_single['status'] = response_json['rows'][0]['status']
        return status_single


if __name__ == '__main__':
    list_code = read_txt('commonbidding_code.txt')
    # 操作类型，1为启动，2为暂停，3为停止，4为重启，5为查看实体当前的调度状态
    option = 1
    # 环境
    # 测试环境：
    # urlTitle = 'http://172.22.69.41:9001/'
    # 生产环境：
    urlTitle = 'http://onestopdata.pactera.com:9001/'

    # 从浏览器获取
    x_user_token = 'eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJQMDEzNDYxNSIsInN1YiI6ImxvZ2luLmxvZ2luIiwiaWF0IjoxNTYzMTU4NjI1LCJhdXRob3JpemF0aW9uIjp7fSwiZGVwdE5hbWUiOiJCRzJfQlVTT0xfU0lJX1Jpc2siLCJsb2dpblRpbWUiOiIyMDE5LTA3LTE1IDEwOjQzOjQ1IiwidW5pdE5hbWUiOiJCRzJfQlVTT0xfU0lJX1Jpc2siLCJ1bml0Q29kZSI6IjEwIiwidGVuYW50SWQiOm51bGwsImRlcHRDb2RlIjoiMTAwNTA3MDQwMyIsImV4cCI6MTU2MzE2MDQyNSwidXNlcm5hbWUiOiLnjovmtanmtIsifQ.abruOq8Bkdvn_wlx34OD5Ij69Pg3kQkAHVLBvUTmvoI'

    schedule_unit = schedule(urlTitle, '', x_user_token)
    if option == 1:
        for code_item in list_code:
            schedule_unit.code = code_item
            schedule_unit.schedule_change('start')
            time.sleep(5)
    elif option == 2:
        for code_item in list_code:
            schedule_unit.code = code_item
            schedule_unit.schedule_change('pause')
    elif option == 3:
        for code_item in list_code:
            schedule_unit.code = code_item
            schedule_unit.schedule_change('stop')
    elif option == 4:
        for code_item in list_code:
            schedule_unit.code = code_item
            schedule_unit.schedule_restart()
    elif option == 5:
        for code_item in list_code:
            schedule_unit.code = code_item
            list_of_details = schedule_unit.schedule_status()
            for item in list_of_details:
                print(item)
