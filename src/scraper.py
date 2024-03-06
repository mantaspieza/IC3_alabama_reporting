import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from collections import OrderedDict
from bs4 import BeautifulSoup, ResultSet
import time


class Scraper:
    """ 
    Class used to scrape data form url provided
    """
    def __init__(
        self,
        url: str = "https://www.ic3.gov/Media/PDF/AnnualReport/2016State/StateReport.aspx#?s=1",
    ) -> None:
        """
        Initiation function for the class

        Args:
            url (str): URL to ic3 gov site. Defaults to "https://www.ic3.gov/Media/PDF/AnnualReport/2016State/StateReport.aspx#?s=1".
        """
        self.url = url
    
    def get_page_response(self) -> ResultSet:
        """
        Function to get the page response as ResultSet object.

        Raises:
            ConnectionError: Returns connection error if there are connection issues.

        Returns:
            BeautifulSoup: ResultSet object used to extract data.
        """
        
        options = Options()
        options.add_argument('--headless=new')
        driver = webdriver.Chrome(options=options)
         
        
        try:
            driver.get(self.url)
            content = driver.page_source
            soup = BeautifulSoup(content, "html.parser")
            driver.close()
        except:
            raise ConnectionError("Check the connection details")
        else:
            return soup


class DataExtractor(Scraper):
    """
    Child to Scraper class, dedicated to extract required data from a single page of IC3

    Args:
        Scraper (class): Parent class to get page response, returning BeautifulSoup object.
    """
    def __init__(
        self,
        year: int,
        state_code: int,
    ) -> None:
        """
        Class Initiation function.

        Args:
            year (int): year of the reporting
            state_code (int): state code of the report
        """
        self.year = year
        self.state_code = state_code
        self.url = f"https://www.ic3.gov/Media/PDF/AnnualReport/{str(self.year)}State/StateReport.aspx#?s={str(self.state_code)}"
        self.page_table_raw_data = None
        self.table_name = None
        self.list_of_tables = []

    def get_table_name(self, iteration: int) -> None:
        """
        Extracts table name from ResultSet

        Args:
            iteration (int): number of the table in a ResultSet
        """
        self.table_name = self.page_table_raw_data[iteration].find("caption").text


    def get_state_name(self, soup: ResultSet) -> None:
        """
        Extracts state name and saves it to the class attribute (state_name) from ResultSet and adjust it to a naming convention.


        Args:
            soup (ResultSet): Object containing single page data
        """
        state_list = soup.findAll("option")
        list_of_states = {int(row["value"]): row.text for row in state_list}
        self.state_name = list_of_states[self.state_code].replace(" ", "_")

    def get_col_names(self, iteration: int) -> None:
        """
        Extracts column names form each table in the page per each iteration.

        Args:
            iteration (int): number of the table in a ResultSet
        """

        tmp_list = []
        for value in self.page_table_raw_data[iteration].find("thead"):
            tmp_list.append(value.text)

        self.columns = list(OrderedDict.fromkeys(tmp_list[1].split("\n")[1:-1]))

    def extract_table_data(self, iteration:int) -> None:
        """
        Extracts contents of a single table in the page per each iteration.

        Args:
            iteration (_type_): number of the table in a ResultSet
        """
        raw_values = []

        for row in self.page_table_raw_data[iteration].find_all("tr"):
            row_data = []
            for cell in row.find_all("td"):
                row_data.append(cell.text)
            raw_values.append(row_data)

        self.raw_values = raw_values

    def filter_table_data(self):
        """
        filters table data from excess data, leaving only required fields.
        """

        self.filtered_records = [row for row in self.raw_values if len(row) > 1]

    def remove_special_characters(self,organized_list:list) -> list:
        """
        Removes special characters ("$", ",") from financial columns.

        Args:
            organized_list (list): list of values organized by the column value.

        Returns:
            list: updated list of values without special characters in financial columns
        """
        for list_ in organized_list:
            list_[-1] = list_[-1].lstrip('$').replace(',','')
        return organized_list
    
    def remove_commas_in_age_grouping(self,organized_list:list) -> list:
        """
        Removes special characters (",") from count column in victims_by_age_group table.

        Args:
            organized_list (list): list of values organized by the column value.

        Returns:
            list: updated list of values without special characters in victims_by_age_group_count column
        """
        for list_ in organized_list:
            list_[-2] = list_[-2].replace(',','')
        
        return organized_list
    
    def arrange_columns(self):
        """ 
        Organizes duplicate column entries, to correct format.
        """
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

        self.page_table_raw_data = soup.findAll("article")
        
        

    def save_extracted_page_data(self) -> None:
        """
        Saves extracted table to staging area as Json file for transformations.
        """

        path = f"./data/staging/{self.year}/{self.state_name}"

        table_name = f'ic3__{self.table_name.lower().replace(' ','_')}'
        if not os.path.isdir(path):
            os.makedirs(path)

    
        pd.DataFrame(data=self.organized_list, columns=list(self.columns)).to_json(f"{path}/{str(table_name)}.json"
        )


    def load_page_to_staging(self):
        """
        Orchestrates scraping single page data, extracting table contents and saves to staging folder.
        """

        self.extract_single_page_info()

        
        for iteration in range(len(self.page_table_raw_data)):


            
            self.get_table_name(iteration=iteration)
            self.list_of_tables.append(self.table_name)

            self.get_col_names(iteration=iteration)

            self.extract_table_data(iteration=iteration)

            self.filter_table_data()

            self.arrange_columns()

            self.save_extracted_page_data()

