import pymysql
import os, sys
rootpath = os.path.abspath(os.path.dirname(__file__))
# print(rootpath)
sys.path.append(rootpath)


def recover_schedule_status(entity_code):
    connect = pymysql.Connect(
        host='172.22.69.43',
        port=3306,
        user='ijep',
        password='ijep#O2018',
        db='ijep',
    )
    cursor = connect.cursor()
    sql5 = f"delete FROM xx where JOB_NAME like '%{entity_code}%';"
    sql2 = f"delete FROM xx where TRIGGER_NAME in (SELECT TRIGGER_NAME FROM sch_triggers where JOB_NAME like '%{entity_code}%');"
    sql3 = f"delete FROM xx WHERE TRIGGER_NAME like '%{entity_code}%';"
    sql4 = f"delete FROM xx WHERE job_name like '%{entity_code}%';"
    sql1 = f"update xx set STATUS_ = 'STOPPED' where CODE_ like '%{entity_code}%';"
    try:
        cursor.execute(sql5)
        cursor.execute(sql2)
        cursor.execute(sql3)
        cursor.execute(sql4)
        cursor.execute(sql1)
    except Exception as e:
        connect.rollback()
        print('执行SQL失败请检查%s'%(e))
    else:
        connect.commit()
        print('执行SQL成功', cursor.rowcount)
    cursor.close()
    connect.close()


def read_txt(path):
    with open(path, 'r') as f:
        content = f.read().splitlines()
    result = []
    for con in content:
        if con.strip() != '':
            result.append(con)
    return result


def run(path):
    entity_code_list = read_txt(path)
    count = 1
    for entity_code in entity_code_list:
        print("正在执行第{}个sql,entity_code:{}".format(count, entity_code))
        recover_schedule_status(entity_code)
        count += 1


if __name__ == '__main__':
    path = f"{rootpath}\code.txt"
    run(path)



