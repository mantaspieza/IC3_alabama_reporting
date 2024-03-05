import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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
        
        options = Options()
        options.add_argument('--headless=new')
        driver = webdriver.Chrome(options=options)
         
        
        try:
            driver.get(self.url)
            content = driver.page_source
            soup = BeautifulSoup(content, "html.parser")
            driver.close()
        except:
            raise ConnectionError("Check the connection")
        else:
            return soup


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
        self.list_of_tables = []

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
    
    def remove_commas_in_age_grouping(self,organized_list:list) -> list:
        for list_ in organized_list:
            list_[-2] = list_[-2].replace(',','')
        
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

        if column_number == 3:
            self.organized_list = self.remove_commas_in_age_grouping(self.organized_list)


    def extract_single_page_info(self) -> None:

        soup = self.get_page_response()
        self.get_state_name(soup)
        # self.get_states_info(soup)
        # self.state_name = self.all_states[self.state_code].replace(" ", "_")

        self.page_table_raw_data = soup.findAll("article")
        
        

    def save_extracted_page_data(self) -> None:

        path = f"./data/staging/{self.year}/{self.state_name}"

        table_name = f'ic3__{self.table_name.lower().replace(' ','_')}'
        if not os.path.isdir(path):
            os.makedirs(path)

    
        pd.DataFrame(data=self.organized_list, columns=list(self.columns)).to_json(f"{path}/{str(table_name)}.json"
        )

    # def extract_state_index_json(self):
    #     self.get_states_info(soup=self.get_page_response())

    #     save_path = f"./documentation/misc/"
    #     file_name = 'state_index.json'

    #     if not os.path.isdir(save_path):
    #         os.makedirs(save_path)

    #     with open(f'{save_path}{file_name}','w') as f:
    #         json.dump(self.all_states, f, ensure_ascii=False)


    def load_page_to_staging(self):

        self.extract_single_page_info()

        
        for iteration in range(len(self.page_table_raw_data)):

            
            self.get_table_name(iteration=iteration)
            self.list_of_tables.append(self.table_name)

            self.get_col_names(iteration=iteration)

            self.extract_table_data(iteration=iteration)

            self.filter_table_data()

            self.arrange_columns()

            self.save_extracted_page_data()

# if __name__ == '__main__':

#     b = DataExtractor(2022,11)
#     b.extract_state_index_json()
        
