from typing import Set
from time import sleep
from alive_progress import alive_bar

from src.scraper import DataExtractor
from src.transformer import DataLoader


class ProcessOrchestrator:
    """
    Class which Orchestrates processes for data extraction and Transformation.
    """

    def __init__(
        self,
        index_range: Set[int] = {i for i in range(1, 58)},
        year_range: Set[int] = {i for i in range(2016, 2023)},
    ) -> None:
        """_summary_

        Args:
            index_range (Set[int]): a Set of state indexes (1-57) used for data scarping.. Defaults to {i for i in range(1, 58)}.
            year_range (Set[int]): a Set of year range (2016-2022) used for data scraping. Defaults to {i for i in range(2016, 2023)}.
        """

        self.index_range = index_range
        self.year_range = year_range

    @property
    def index_range(self):
        return self._index_range

    @index_range.setter
    def index_range(self, value: Set[int]):
        """
        Index_range property setter to validate state indexes.

        Args:
            value (Set[int]): a Set of state indexes as int

        Raises:
            Exception: Checks if type of the value is set. Raises TypeError
            Exception: Checks if all objects in set are int. Raises TypeError
            Exception: Cheks if range restrictions. Raises ValueError
        """
        if not isinstance(value, set):
            raise Exception(
                TypeError(
                    "index_range should be presented as a set of integers (Set[int])"
                )
            )
        for i in value:
            if not isinstance(i, int):
                raise Exception(
                    TypeError(
                        "values within index_range set is expected to be integers"
                    )
                )

        if min(value) < 1 or max(value) > 57:
            raise Exception(ValueError("State index values from 1 to 57 are accepted"))

        self._index_range = value

    @property
    def year_range(self):
        return self._year_range

    @year_range.setter
    def year_range(self, value: Set[int]):
        """
        year_range property setter to validate year range.

        Args:
            value (Set[int]): a Set of year_range as int

        Raises:
            Exception: Checks if type of the value is set. Raises TypeError
            Exception: Checks if all objects in set are int. Raises TypeError
            Exception: Cheks if range restrictions. Raises ValueError
        """
        if not isinstance(value, set):
            raise Exception(
                TypeError(
                    "year_range should be presented as a set of integers (Set[int])"
                )
            )
        for i in value:
            if not isinstance(i, int):
                raise Exception(
                    TypeError("values within year_range set is expected to be integers")
                )

        if min(value) < 2016 or max(value) > 2022:
            raise Exception(ValueError("year values from 2016 to 2022 are accepted"))

        self._year_range = value

    def get_single_state_data(self, year: int, state_code: int) -> None:
        """
        Orchestrates extraction of a single page data using DataExtractor class, applies required transformations using DataLoader class.

        Args:
            year (int): year of the report
            state_code (int): state code of the report
        """

        state_info = DataExtractor(year=year, state_code=state_code)
        state_info.load_page_to_staging()

        self.state_name = state_info.state_name

        DataLoader(year=year, state_name=self.state_name).transform_tables_in_folder()

    def get_all_data(self):
        """
        Orchestrates extraction of all requested data.
        """
        if len(self.year_range) > 1 and len(self.index_range) > 1:
            for year in self.year_range:
                with alive_bar(57) as bar:
                    for state in self.index_range:

                        self.get_single_state_data(year=year, state_code=state)
                        sleep(1)
                        bar()

        elif len(self.year_range) == 1 and len(self.index_range) > 1:

            for state in self.index_range:
                self.get_single_state_data(
                    year=list(self.year_range)[0], state_code=state
                )
                sleep(1)
            print("load completed")

        elif len(self.year_range) > 1 and len(self.index_range) == 1:
            for year in self.year_range:
                self.get_single_state_data(
                    year=year, state_code=list(self.index_range)[0]
                )
                sleep(1)
            print("load completed")
