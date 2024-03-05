import os
import shutil
import pandas as pd

from typing import Set


class DataLoader:

    def __init__(
        self,
        # year_range: Set[int],
        # index_range: Set[int],
        year: int = None,
        state_name: int = None,
    ) -> None:
        # self.year_range = year_range
        # self.index_range = index_range
        self.year = year
        self.state_name = state_name
        self.staging_path = f"data/staging/{str(year)}/{str(state_name)}/"
        self.loading_path = f"data/{str(year)}/{str(state_name)}/"
        self.files: Set[str] = {}

    def collect_folder_content(self) -> None:

        self.files = set(os.listdir(self.staging_path))

    def read_data(self, file_path, predfined_dtypes) -> pd.DataFrame:

        dataframe = pd.read_json(file_path, dtype=predfined_dtypes)

        return dataframe

    def save_data(
        self, location: str, table_name: str, dataframe: pd.DataFrame
    ) -> None:

        if not os.path.isdir(self.loading_path):
            os.makedirs(self.loading_path)

        dataframe.to_parquet(f"{location}{table_name}.parquet")

    def transform_single_table(self, file_name: str) -> None:

        file_path = f"{self.staging_path}{str(file_name)}"
        table_name = file_name[:-5]

        if file_name == "ic3__victims_by_age_group.json":
            predefined_dtypes = {"Age Range": str, "Count": int, "Amount Loss": int}

        elif file_name == "ic3__crime_type_by_subject_count.json":
            predefined_dtypes = {"Crime Type": str, "Subject Count": int}

        elif file_name == "ic3__crime_type_by_victim_count.json":
            predefined_dtypes = {"Crime Type": int, "Victim Count": int}

        else:
            predefined_dtypes = {"Crime Type": str, "Loss Amount": int}

        self.save_data(
            location=self.loading_path,
            table_name=table_name,
            dataframe=self.read_data(
                file_path=file_path, predfined_dtypes=predefined_dtypes
            ),
        )

    def transform_tables_in_folder(self):

        self.collect_folder_content()
        for table in self.files:

            self.transform_single_table(file_name=str(table))

        self.remove_used_staging_folder()

    def remove_used_staging_folder(self):

        if os.path.isdir("data/staging/"):
            shutil.rmtree("data/staging/")
