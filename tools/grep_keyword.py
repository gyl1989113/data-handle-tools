#!/usr/bin/python
# -*- encoding: utf-8 -*-
# version 1.0
import re
import time
 
'''
用于筛选日志文件，适用于python2.x版本
使用时将日志文件放于search.py工具同一目录
筛选完毕后会出现“旧文件名+当前时间”格式命名的新日志文件
'''
 
 
def getParameters():
    file_name = ""
    key_work = ""
    while (True):
        file_name = raw_input("请输入文件名：")
        key_work = raw_input("请输入过滤关键字：")
        if len(file_name) == 0 or len(key_work) == 0:
            flag = raw_input("您输入的文件名或关键子为空，输出c重试，q退出程序：")
            if flag == "q":
                return
            elif flag == "c":
                continue
        else:
            break
 
    new_file = file_name + "-" + formatTime(time.localtime())
    f = open("./" + file_name, "rb")
    lines = f.readlines()
 
    if len(lines) == 0:
        print("========日志文件为空========")
        f.close()
        return
 
    nf = open("./" + new_file, "wb");
    count = 0
    for line in lines:
        rs = re.search(key_work, line)
        if rs:
            print("[命中]--->%s" % line)
            nf.write(line)
            count = count + 1
 
    f.close()
    nf.close()
    print("共找到%d条信息" % count)
 
 
def formatTime(timevalue):
    '''
    format the time numbers
    '''
    return time.strftime("%Y%m%d%H%M%S", timevalue)
 
 
if __name__ == '__main__':
    getParameters()

