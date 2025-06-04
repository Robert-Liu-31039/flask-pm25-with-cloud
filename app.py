from flask import Flask, render_template, request
from datetime import datetime, timedelta
import pymysql
import pandas as pd
from pm25 import (
    get_pm25_data_from_mysql,
    update_db,
    get_pm25_data_by_site,
    get_all_counties,
    get_all_sites,
    get_site_by_county,
)
import json

# __name__ <- 指的就是本頁
app = Flask(__name__)


# 宣告首頁的路徑
@app.route("/")
# 宣告首頁立刻被執行的 function 方法
def index():
    # 取得資藥庫最新資料
    columns, datas = get_pm25_data_from_mysql()

    # 取出不同縣市
    df = pd.DataFrame(datas, columns=columns)
    counties = sorted(df["county"].unique().tolist())

    # 選取縣市後的資料(預設值 ALL)
    county = request.args.get("county", "ALL")

    if county == "ALL":
        # 因為如果是 ALL 的呈現時，資料的筆試會過多，
        # 所以畫面會很難看，所以將資料改為只取 county 的平均值

        # 因為被 取完平均值後的 col index 會被自動消失，
        # 所以透過 .reset_index() 的函式功能，將 col name 給回推回去重現!
        df1 = df.groupby("county")["pm25"].mean().reset_index()

        # 繪製所需資料
        x_data = df1["county"].tolist()
    else:
        df = df.groupby("county").get_group(county)

        # 繪製所需資料
        x_data = df["site"].tolist()

    y_data = df["pm25"].tolist()
    columns = df.columns.tolist()
    datas = df.values.tolist()

    # **locals() -> 會把區域端的所有變數全部都丟過去，
    # 好處是 : 當要傳遞的參數很多時，不用一個一個寫
    # 壞處是 : 效能會變差
    return render_template("index.html", **locals())


# 宣告 form 使用 POST 傳遞資料
@app.route("/filter", methods=["POST"])
def filter_data():
    # form 使用 POST 傳遞資料，接收的寫法就要改為 : request.form.get("@參數名稱")
    county = request.form.get("county")

    columns, datas = get_pm25_data_from_mysql()

    # 取得該縣市的資料
    df = pd.DataFrame(datas, columns=columns)
    df1 = df.groupby("county").get_group(county)
    print(df1)
    return {"county": county}


# 宣告網頁的路徑
@app.route("/books")
# 宣告網頁立刻被執行的 function 方法
def books_page():
    # return f"<h1>Hello World!</h1><br>{datetime.now()}"
    books = {1: "Python book", 2: "Java book", 3: "Flask book"}
    for key in books:
        print(key, books[key])

    books_2 = [
        {
            "name": "Python book",
            "price": 299,
            "image_url": "https://im2.book.com.tw/image/getImage?i=https://www.books.com.tw/img/CN1/136/11/CN11361197.jpg&v=58096f9ck&w=348&h=348",
        },
        {
            "name": "Java book",
            "price": 399,
            "image_url": "https://im1.book.com.tw/image/getImage?i=https://www.books.com.tw/img/001/087/31/0010873110.jpg&v=5f7c475bk&w=348&h=348",
        },
        {
            "name": "C# book",
            "price": 499,
            "image_url": "https://im1.book.com.tw/image/getImage?i=https://www.books.com.tw/img/001/036/04/0010360466.jpg&v=62d695bak&w=348&h=348",
        },
    ]

    books_2 = []

    if books_2:
        for book in books_2:
            print(book["name"])
            print(book["price"])
            print(book["image_url"])
    else:
        print("販售完畢，目前無書籍!")

    username = "jerry"
    nowtime = datetime.now().strftime("%Y-%m-%d")
    print(username, nowtime)
    return render_template(
        "books.html", name=username, now=nowtime, books=books, books_2=books_2
    )


@app.route("/pm25-data")
def get_pm25_data():
    api_url = "https://data.moenv.gov.tw/api/v2/aqx_p_02?api_key=540e2ca4-41e1-4186-8497-fdd67024ac44&limit=1000&sort=datacreationdate%20desc&format=CSV"
    datas = pd.read_csv(api_url, encoding="utf-8-sig")
    datas["datacreationdate"] = pd.to_datetime(datas["datacreationdate"])
    df = datas.drop(datas[datas["pm25"].isna()].index)
    return df.values.tolist()


@app.route("/bmi")
def get_bmi():
    # request.args.get("@參數名稱) -> 根據 form 所回傳的參數內容，去做參數的值的接取
    height = eval(request.args.get("height"))
    weight = eval(request.args.get("weight"))

    bmi = round(weight / (height / 100) ** 2, 2)
    # return {"height": height, "weight": weight, "bmi": bmi}
    # return render_template("bmi.html", height=height, weight=weight, bmi=bmi)

    # **locals() -> 會把區域端的所有變數全部都丟過去，
    # 好處是 : 當要傳遞的參數很多時，不用一個一個寫
    # 壞處是 : 效能會變差
    return render_template("bmi.html", **locals())


# 更新資料庫
@app.route("/update-db")
def update_pm25_db():

    # 因為上雲端後，雲端的 Server 讀取的是 utc 的時間，
    # 而台灣是 utc+8 的時間，
    # 所以要使用 datetime.now()+timedelta(hours=8) 來取得台灣時間
    nowtime = (datetime.now() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    count, message = update_db()
    info = {"時間": nowtime, "更新筆數": count, "結果": message}

    # 因為回傳的資訊有中文，所以使用 json.dumps() 的 ensure_ascii=False 的設定來做轉碼
    result = json.dumps(info, ensure_ascii=False)

    return result


@app.route("/pm25-data-site")
def pm25_data_by_site():
    county = request.args.get("county")
    site = request.args.get("site")

    if not county or not site:
        # 因為回傳的資訊有中文，所以使用 json.dumps() 的 ensure_ascii=False 的設定來做轉碼
        result = json.dumps({"error": "縣市跟站點名稱不能為空!"}, ensure_ascii=False)
    else:
        columns, datas = get_pm25_data_by_site(county=county, site=site)

        # 取得該縣市的資料
        df = pd.DataFrame(datas, columns=columns)
        date = df["datacreationdate"].apply(lambda x: x.strftime("%Y-%m-%d %H"))

        result = json.dumps(
            {
                "county": county,
                "site": site,
                "x_data": date.tolist(),
                "y_data": df["pm25"].tolist(),
                "higher": df["pm25"].max(),
                "lower": df["pm25"].min(),
            },
            ensure_ascii=False,
        )

    return result


@app.route("/pm25-site")
def pm25_site():
    counties = get_all_counties()
    sites = get_all_sites()
    return render_template("pm25-site.html", counties=counties, sites=sites)


@app.route("/pm25-county-site")
def pm25_county_site():
    county = request.args.get("county")
    sites = get_site_by_county(county)

    result = json.dumps(sites, ensure_ascii=False)

    return result


# 當程式是跑在本地運行的時候，才會跑以下的程式碼，
# 不然若是有其他程式 call 你這支程式碼時，
# 就會把以下的程式自動 run 起來了(誤跑)!
if __name__ == "__main__":
    # 讓 Flask Server run 起來
    app.run(debug=False)
