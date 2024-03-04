import os
import requests
import pandas as pd
from selenium import webdriver
from collections import OrderedDict
from bs4 import BeautifulSoup, ResultSet
import time


class Scraper:
    def __init__(
        self,
        url: str = "https://www.ic3.gov/Media/PDF/AnnualReport/2016State/StateReport.aspx#?s=1",
    ) -> None:
        self.url = url

    def get_page_response(self) -> BeautifulSoup:
        driver = webdriver.Chrome()
        try:
            driver.get(self.url)
            content = driver.page_source
            soup = BeautifulSoup(content, "html.parser")
            driver.close()
            return soup
        except:
            raise ConnectionError("Check the connection")


class DataExtractor(Scraper):
    def __init__(
        self,
        year: int,
        state_code: int,
    ) -> None:
        self.year = year
        self.state_code = state_code
        self.url = f"https://www.ic3.gov/Media/PDF/AnnualReport/{str(self.year)}State/StateReport.aspx#?s={str(self.state_code)}"
        self.page_table_raw_data = None
        self.table_name = None

    def get_table_name(self, iteration) -> None:
        self.table_name = self.page_table_raw_data[iteration].find("caption").text

    def get_state_name(self, soup: ResultSet) -> None:
        state_list = soup.findAll("option")
        list_of_states = {int(row["value"]): row.text for row in state_list}
        self.state_name = list_of_states[self.state_code].replace(" ", "_")

    def get_col_names(self, iteration) -> None:

        tmp_list = []
        for value in self.page_table_raw_data[iteration].find("thead"):
            tmp_list.append(value.text)

        self.columns = list(OrderedDict.fromkeys(tmp_list[1].split("\n")[1:-1]))

    def extract_table_data(self, iteration) -> None:
        raw_values = []

        for row in self.page_table_raw_data[iteration].find_all("tr"):
            row_data = []
            for cell in row.find_all("td"):
                row_data.append(cell.text)
            raw_values.append(row_data)

        self.raw_values = raw_values

    def filter_table_data(self):

        self.filtered_records = [row for row in self.raw_values if len(row) > 1]

    def remove_special_characters(self,organized_list:list) -> list:
        for list_ in organized_list:
            list_[-1] = list_[-1].lstrip('$').replace(',','')
        return organized_list
    
    def arrange_columns(self):
        organized_list = []
        start = 0
        column_number = len(self.columns)

        for item in self.filtered_records:
            end = len(item)
            for i in range(start, end, column_number):
                organized_list.append(item[i : i + column_number])

        self.organized_list = self.remove_special_characters(organized_list)

    def extract_all_raw_table_data(self) -> None:

        soup = self.get_page_response()
        self.page_table_raw_data = soup.findAll("article")
        self.get_state_name(soup)
        

    def save_raw_data(self) -> None:
        path = f"data/{self.year}/{self.state_name}"
        table_name = f'ic3__{self.table_name.lower().replace(' ','_')}'
        if not os.path.isdir(path):
            os.makedirs(path)

        pd.DataFrame(data=self.organized_list, columns=list(self.columns)).to_json(f"{path}/{str(table_name)}.json"
        )

    def load_page_data_to_bronze(self):

        self.extract_all_raw_table_data()

        for iteration in range(len(self.page_table_raw_data)):
            self.get_table_name(iteration=iteration)
            self.get_col_names(iteration=iteration)

            self.extract_table_data(iteration=iteration)

            self.filter_table_data()

            self.arrange_columns()

            self.save_raw_data()
            


if __name__ == "__main__":
    pass
    scrp = DataExtractor(year=2022, state_code=44)

    scrp.load_page_data_to_bronze()
