import requests
from bs4 import BeautifulSoup


def get_url_attr(url, name, attrs):
    res = requests.get(url)

    if res.status_code == 200:
        obj = BeautifulSoup(res.text, 'html.parser')
        # attr = obj.find(name="div", attrs={'id':'auto-channel-lazyload-article'})
        div_obj = obj.find(name=name, attrs=attrs)
        li_list = div_obj.find_all("li")
        # print(li_list)
        for i in li_list[:1]:
            if not i: continue
            title = i.find('h3')
            summary = i.find('p')
            url = "https:" + i.find('a').get('href')
            print(url)
            tag = url.split('/',4)[3]
            print(tag)
    return li_list


if __name__ == '__main__':
    url = "https://www.autohome.com.cn/all/2/#liststart"
    name = "div"
    attrs = {'id':'auto-channel-lazyload-article'}
    li_list = get_url_attr(url, name, attrs)