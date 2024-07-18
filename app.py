import os
from typing import List, Dict

import requests


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


class Countries(ETLModel):
    @classmethod
    def get_data(cls):
        access_token = cls.get_access_token()
        url = f"{cls.API_URL}/countries/"

        response = requests.get(url, headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        })
        return response.json()

    @classmethod
    def get_sql_insert(cls) -> str:
        data: List[Dict[str, str]] = cls.get_data()
        sql = "INSERT INTO countries(country_name, country_short_name) VALUES "

        for country in data:
            c_name = country["country_name"].replace("'", " ")
            c_short_name = country["country_short_name"]

            sql += f"""('{c_name}', '{c_short_name}'), """

        sql = sql[:-2]

        print(sql)


class States(ETLModel):
    @classmethod
    def get_data(cls, countries: List[str]):
        base_url = f"{cls.API_URL}/states/"

        for country in countries:
            try:
                url = base_url + country
                response = requests.get(url, headers={
                    "Authorization": f"Bearer {cls.get_access_token()}",
                    "Accept": "application/json"
                })
                data = response.json()
                print(country)
                print(data)
            except:
                print(f"Fallo para {country}")


class Cities(ETLModel):
    ...


def main():
    countries = [country["country_name"] for country in Countries.get_data()]
    States.get_data(countries)


if __name__ == "__main__":
    main()
