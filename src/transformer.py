import os
import shutil
import pandas as pd

from typing import Set


class DataLoader:
    """
    Class to load data from staging and transform to parquet file format
    """

    def __init__(
        self,
        year: int = None,
        state_name: str = None,
    ) -> None:
        """
        Initiation function for DataLoader

        Args:
            year (int): year of the report. Defaults to None.
            state_name (str): name of the state. Defaults to None.
        """

        self.year = year
        self.state_name = state_name
        self.staging_path = f"data/staging/{str(year)}/{str(state_name)}/"
        self.loading_path = f"data/{str(year)}/{str(state_name)}/"
        self.files: Set[str] = {}

    def collect_folder_content(self) -> None:
        """
        Collects local files from staging folder.
        """

        self.files = set(os.listdir(self.staging_path))

    def read_data(self, file_path: str, predfined_dtypes: dict) -> pd.DataFrame:
        """
        Reads in json file from staging area, applying required datatypes for table

        Args:
            file_path (str): File path to Json file
            predfined_dtypes (dict): Contains predefined column data types

        Returns:
            pd.DataFrame: containing table information from Json file.
        """

        dataframe = pd.read_json(file_path, dtype=predfined_dtypes)

        return dataframe

    def save_data(
        self, location: str, table_name: str, dataframe: pd.DataFrame
    ) -> None:
        """
        Saves table as a .parquet file to a designated location.

        Args:
            location (str): designated location to save file.
            table_name (str): filename for table to be saved.
            dataframe (pd.DataFrame): contains transformed data from staging json file.
        """

        if not os.path.isdir(self.loading_path):
            os.makedirs(self.loading_path)

        dataframe.to_parquet(f"{location}{table_name}.parquet")

    def transform_single_table(self, file_name: str) -> None:
        """
        Loads single file from staging area, applies transformations and saves it as a parquet file.

        Args:
            file_name (str): identifies Json file in a folder
        """

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
        """
        Orchestrates all file transformation per folder. removes staging folder after completion.
        """

        self.collect_folder_content()
        for table in self.files:

            self.transform_single_table(file_name=str(table))

        self.remove_used_staging_folder()

    def remove_used_staging_folder(self):
        """
        Removes staging files from local storage after Json files are transformed.
        """

        if os.path.isdir("data/staging/"):
            shutil.rmtree("data/staging/")
