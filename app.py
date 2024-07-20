import os
from typing import List, Dict

import requests
import pymysql.cursors
from pymysql.err import OperationalError


API_TOKEN = os.getenv("COUNTRIES_API_TOKEN") or "Ri7hUvhTlWBbLXBmruBKMXcgkeudXO79RuPXd44n_eYNLDquyZtbPg8_UKh2n8cKqqo"
API_EMAIL = os.getenv("COUNTRIES_API_USER_EMAIL") or "joaquin.p.olivera@gmail.com"


class ETLModel:
    API_URL = "https://www.universal-tutorial.com/api"

    @classmethod
    def get_access_token(cls) -> str:
        url = f"{cls.API_URL}/getaccesstoken"
        headers = {
            "Accept": "application/json",
            "api-token": API_TOKEN,
            "user-email": API_EMAIL
        }

        response = requests.get(url, headers=headers)
        data = response.json()
        return data.get("auth_token")

    @classmethod
    def execute_query(cls, sql_query: str, return_data=False):
        try:
            connection = pymysql.connect(
                host="localhost",
                port=3306,
                database="tour_dates",
                user="admin",
                password="admin"
            )

            with connection.cursor() as cursor:
                cursor.execute(sql_query)

                if not return_data:
                    connection.commit()
                    return None

                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                res = [dict(zip(cols, row)) for row in rows]
                return res

        except OperationalError as e:
            print(f"OperationalError: {e}")
            return None
        finally:
            connection.close()


class Countries(ETLModel):
    @classmethod
    def run(cls, only_return_data=False):
        access_token = cls.get_access_token()
        url = f"{cls.API_URL}/countries/"

        response = requests.get(url, headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        })

        data = response.json()

        if only_return_data:
            return data
        cls.insert_data(data)

    @classmethod
    def insert_data(cls, data: List[Dict[str, str]]) -> str:
        sql = "INSERT INTO countries(country_name, country_short_name) VALUES "

        for country in data:
            c_name = country["country_name"].replace("'", " ")
            c_short_name = country["country_short_name"]

            sql += f"""('{c_name}', '{c_short_name}'), """

        sql = sql[:-2]

        print(sql)


class States(ETLModel):
    @classmethod
    def run(cls):
        countries: List[str] = [country["country_name"] for country in Countries.run(True)]

        base_url = f"{cls.API_URL}/states/"

        for country in countries:
            try:
                url = base_url + country
                response = requests.get(url, headers={
                    "Authorization": f"Bearer {cls.get_access_token()}",
                    "Accept": "application/json"
                })
                print("Procesando", country)
                data = response.json()
                states = [state["state_name"] for state in data]
                cls.insert_data(states, country)
            except:
                print(f"Fallo para {country}")

    @classmethod
    def insert_data(cls, data: List[str], country: str):
        for state_name in data:
            sql = "INSERT INTO states(state_name, country_id) \n"
            sql += f"VALUES ('{state_name}', (SELECT country_id FROM countries WHERE country_name = '{country}'))"
            print(sql)
            try:
                cls.execute_query(sql)
            except Exception as error:
                print(error)


class Cities(ETLModel):
    @classmethod
    def get_all_registred_states(cls) -> List[str]:
        data = cls.execute_query("SELECT DISTINCT state_name FROM states", True)
        return [state["state_name"] for state in data]

    @classmethod
    def run(cls):
        states = cls.get_all_registred_states()

        base_url = f"{cls.API_URL}/cities/"

        for state in states:
            try:
                url = base_url + state
                response = requests.get(url, headers={
                    "Authorization": f"Bearer {cls.get_access_token()}",
                    "Accept": "application/json"
                })
                print("Procesando", state)
                data = response.json()
                cities = [city["city_name"] for city in data]
                print(cities)
                cls.insert_data(cities, state)
            except:
                print(f"Fallo para {state}")

    @classmethod
    def insert_data(cls, data: List[str], state: str):
        x = 0
        for city_name in data:
            sql = "INSERT INTO cities(city_name, state_id) \n"
            sql += f"VALUES ('{city_name}', (SELECT state_id FROM states WHERE state_name = '{state}'))"
            print(sql)
            try:
                cls.execute_query(sql)
            except Exception as error:
                print(error)

def main():
    # countries = [country["country_name"] for country in Countries.get_data()]
    # States.get_data(countries)
    Cities.run()


if __name__ == "__main__":
    main()
