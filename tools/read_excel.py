import xlrd
import time
import csv
import os


def read_excel_content(text):
    workbook = xlrd.open_workbook(file_contents=text, formatting_info=True)
    return workbook


def read_excel_file(file):
    workbook = xlrd.open_workbook(file)
    return workbook

COUNT = 0

CHECK_LIST = list()

TIME_ARRARY = time.localtime()

TIME = time.strftime("%Y-%m-%d")

DIR_PATH = "./"

FILE_PATH = f"{DIR_PATH}wechat{TIME}.csv"


def to_csv(data, file_path=None):
    if not file_path:
        file_path = FILE_PATH

    if not CHECK_LIST:
        for each_key in data:
            CHECK_LIST.append(each_key)

    global COUNT
    COUNT += 1
    if os.path.exists(file_path):
        with open(file_path, "a", newline="", errors="ignore") as f:
            writer = csv.writer(f)
            append_list = list()
            for key in CHECK_LIST:
                try:
                    append_list.append(data[key])
                except KeyError:
                    append_list.append("")
            writer.writerows([append_list])
    else:
        try:
            with open(file_path, "a", newline="", errors="ignore") as f:
                writer = csv.writer(f)
                check_list = list()
                append_list = list()
                for key in CHECK_LIST:
                    check_list.append(key)
                    try:
                        append_list.append(data[key])
                    except KeyError:
                        append_list.append("")
                writer.writerows([check_list, append_list])
        except FileNotFoundError:
            os.makedirs(DIR_PATH)
            with open(file_path, "a", newline="", errors="ignore") as f:
                writer = csv.writer(f)
                check_list = list()
                append_list = list()
                for key in CHECK_LIST:
                    check_list.append(key)
                    try:
                        append_list.append(data[key])
                    except KeyError:
                        append_list.append("")
                writer.writerows([check_list, append_list])


if __name__ == '__main__':

    data_list = list()
    workbook = read_excel_file(r"D:\wechat.xlsx")
    sheetname = workbook.sheet_names()[0]
    sheet = workbook.sheet_by_name(sheetname)
    row_list = sheet.row_values(0)
    # print(row_list)
    for n in range(1, sheet.nrows):
        data_item = dict()
        row_data = sheet.row_values(n)
        data_item = dict(zip(row_list, row_data))
        # print(data_item)
        data_list.append(data_item)
    print(data_list)

