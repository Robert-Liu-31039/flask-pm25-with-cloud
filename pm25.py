import pymysql
import pandas as pd

from dotenv import load_dotenv
import os

load_dotenv(".env")


def open_db():
    conn = None
    try:
        conn = pymysql.connect(
            host=os.environ.get("DB_HOST"),
            port=int(os.environ.get("DB_PORT")),
            user=os.environ.get("DB_USER"),
            passwd=os.environ.get("DB_PASSWORD"),
            db=os.environ.get("DB_NAME"),
        )
    except Exception as ex:
        print("資料庫開啟失敗", ex)

    return conn


def get_pm25_data_from_mysql():
    conn = None
    columns, datas = None, None

    try:
        conn = open_db()
        cursor = conn.cursor()

        sqlstr = "select * from pm25 where datacreationdate = (select MAX(datacreationdate) from pm25);"
        cursor.execute(sqlstr)

        # 取得 Table 的欄位名稱
        # print(cursor.description)
        columns = [col[0] for col in cursor.description]

        # 實際的資料
        datas = cursor.fetchall()
    except Exception as ex:
        print(ex)
    finally:
        if conn is not None:
            conn.close()

    return columns, datas


# 更新資料庫
def update_db():
    conn = None
    api_url = "https://data.moenv.gov.tw/api/v2/aqx_p_02?api_key=540e2ca4-41e1-4186-8497-fdd67024ac44&limit=1000&sort=datacreationdate%20desc&format=CSV"
    sqlstr = """
insert ignore into pm25(site, county, pm25, datacreationdate, itemunit) 
values(%s,%s,%s,%s,%s);
"""
    row_count = 0
    message = ""

    try:
        # 讀取最新的雲端資料
        datas = pd.read_csv(api_url, encoding="utf-8-sig")
        datas["datacreationdate"] = pd.to_datetime(datas["datacreationdate"])
        df = datas.drop(datas[datas["pm25"].isna()].index)
        values = df.values.tolist()

        # 寫入資料庫
        conn = open_db()
        cursor = conn.cursor()
        cursor.executemany(sqlstr, values)
        row_count = cursor.rowcount
        conn.commit()

        print(f"更新{row_count}筆資料成功!")
        message = "更新資料庫成功!"

    except Exception as ex:
        print(ex)
        message = f"更新資料庫失敗{ex}"
    finally:
        if conn is not None:
            conn.close()

    return row_count, message


# 取得縣市對應的 site 資料
def get_pm25_data_by_site(county, site):
    conn = None
    columns, datas = None, None

    try:
        conn = open_db()
        cursor = conn.cursor()

        # 使用 %s 傳入變數的值
        sqlstr = "select * from pm25 where county = %s and site = %s;"
        #  在 execute 中，變數哪怕只有一個也要是寫這樣的格式 (@變數,)
        cursor.execute(sqlstr, (county, site))

        # 取得 Table 的欄位名稱
        # print(cursor.description)
        columns = [col[0] for col in cursor.description]

        # 實際的資料
        datas = cursor.fetchall()
    except Exception as ex:
        print(ex)
    finally:
        if conn is not None:
            conn.close()

    return columns, datas


def get_all_counties():
    conn = None
    counties = []

    try:
        conn = open_db()
        cursor = conn.cursor()

        sqlstr = "select distinct county from pm25;"
        cursor.execute(sqlstr)
        datas = cursor.fetchall()

        counties = [data[0] for data in datas]
    except Exception as ex:
        print(ex)
    finally:
        if conn is not None:
            conn.close()

    return counties


def get_all_sites():
    conn = None
    sites = []

    try:
        conn = open_db()
        cursor = conn.cursor()

        sqlstr = "select distinct site from pm25;"
        cursor.execute(sqlstr)
        datas = cursor.fetchall()

        sites = [data[0] for data in datas]
    except Exception as ex:
        print(ex)
    finally:
        if conn is not None:
            conn.close()

    return sites


def get_site_by_county(county):
    conn = None
    sites = []

    try:
        conn = open_db()
        cursor = conn.cursor()

        sqlstr = "select distinct site from pm25 where county=%s;"
        #  在 execute 中，變數哪怕只有一個也要是寫這樣的格式 (@變數,)
        cursor.execute(sqlstr, (county,))
        datas = cursor.fetchall()

        sites = [data[0] for data in datas]
    except Exception as ex:
        print(ex)
    finally:
        if conn is not None:
            conn.close()

    return sites


# 當程式是跑在本地運行的時候，才會跑以下的程式碼，
# 不然若是有其他程式 call 你這支程式碼時，
# 就會把以下的程式自動 run 起來了(誤跑)!
if __name__ == "__main__":
    # columns, datas = get_pm25_data_from_mysql()
    # print(columns, datas)

    # update_db()
    # print(get_pm25_data_by_site("新北市", "富貴角"))
    # print(get_all_counties())
    print(get_site_by_county("新北市"))
