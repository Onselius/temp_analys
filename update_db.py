import sqlite3
import requests
import json
import datetime


def main():
    connection = sqlite3.connect("iot.db")
    cursor = connection.cursor()
    create_table(cursor)
    temp_data = get_data_from_api()
    update_table_with_data(cursor, temp_data, 127310)

    connection.commit()
    connection.close()
    
def create_table(cursor: sqlite3.Cursor):
    try:
        cursor.execute("CREATE TABLE temp(stationid INTEGER, date INTEGER, value REAL, quality TEXT)")
    except sqlite3.OperationalError:
        pass

def update_table_with_data(cursor: sqlite3.Cursor, data: str, station_id: int):
    for temp in data:
        cursor.execute("INSERT INTO temp VALUES(?,?,?,?)", (station_id, temp["date"], temp["value"], temp["quality"]))

def get_data_from_api():
    url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/1/station/127310/period/latest-months/data.json"
    response = requests.get(url)
    json_response = response.json()
    with open("temp.txt", "w") as file:
        for station in json_response["value"]:
            file.write(f'{station["date"]} {station["value"]}\n')
    return json_response["value"]

# Hjälpfunktion för att navigera i API:et
def get_stations_from_api():
    url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/1.json"
    response = requests.get(url)
    json_response = response.json()
    with open("stations.txt", "w") as file:
        for station in json_response["station"]:
            file.write(station["name"] + " " + station["key"] + "\n")

# Hjälpfunktion för att navigera i API:et
def get_period_from_api():
    url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/1/station/127310.json"
    response = requests.get(url)
    json_response = response.json()
    with open("periods.txt", "a") as file:
        file.write(json_response["key"] + " " + json_response["title"] + "\n")
        for station in json_response["period"]:
            file.write(station["key"] + " " + station["title"] + "\n")

# Hämtar de senaste 50 posterna
def test_sql_connection():
    connection = sqlite3.connect("iot.db")
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM temp ORDER BY date desc LIMIT 50")
    for row in cursor.fetchall():
        human_date = datetime.datetime.fromtimestamp(row[1] / 1000)
        print(f"{human_date} {row[2]} {row[3]}")


if __name__ == "__main__":
#    get_data_from_api()
#    get_stations_from_api()
#    get_period_from_api()
    main()
    test_sql_connection()

