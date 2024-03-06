from typing import Set
from time import sleep
from alive_progress import alive_bar

from src.scraper import DataExtractor
from src.transformer import DataLoader


class ProcessOrchestrator:

    def __init__(
        self,
        index_range: Set[int] = {i for i in range(1, 58)},
        year_range: Set[int] = {i for i in range(2016, 2023)},
    ) -> None:

        self.index_range = index_range
        self.year_range = year_range

    @property
    def index_range(self):
        return self._index_range

    @index_range.setter
    def index_range(self, value):
        if not isinstance(value, set):
            raise Exception(
                TypeError(
                    "index_range should be presented as a set of integers (Set[int])"
                )
            )
        for i in value:
            if not isinstance(i, int):
                raise Exception(
                    "values within index_range set is expected to be integers"
                )

        if min(value) < 1 or max(value) > 57:
            raise Exception(ValueError("State index values from 1 to 57 are accepted"))

        self._index_range = value

    @property
    def year_range(self):
        return self._year_range

    @year_range.setter
    def year_range(self, value):
        if not isinstance(value, set):
            raise Exception(
                TypeError(
                    "year_range should be presented as a set of integers (Set[int])"
                )
            )
        for i in value:
            if not isinstance(i, int):
                raise Exception(
                    "values within year_range set is expected to be integers"
                )

        if min(value) < 2016 or max(value) > 2022:
            raise Exception(ValueError("year values from 2016 to 2022 are accepted"))

        self._year_range = value

    def get_single_state_data(self, year: int, state_code: int) -> None:

        state_info = DataExtractor(year=year, state_code=state_code)
        state_info.load_page_to_staging()

        self.state_name = state_info.state_name

        DataLoader(year=year, state_name=self.state_name).transform_tables_in_folder()

    def get_all_data(self):
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

        elif len(self.year_range) > 1 and len(self.index_range) == 1:
            for year in self.year_range:
                self.get_single_state_data(
                    year=year, state_code=list(self.index_range)[0]
                )
                sleep(1)
