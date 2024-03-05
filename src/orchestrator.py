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
