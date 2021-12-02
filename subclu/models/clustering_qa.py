"""
Utilities to reshape & apply QA to clustering outputs
"""
import pandas as pd
# auth & utils for google sheets
import gspread
from gspread.spreadsheet import Spreadsheet
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from oauth2client.client import GoogleCredentials


class CustomGoogleSheet(Spreadsheet):
    """
    Custom class wrapper around the gspread library to make it easier to:
    - create & read google sheet documents
    - create, download, & upload data from specific tabs

    This should help with standardizing specific tabs/worksheet.
    """
    def __init__(
            self,
            gsheet_name: str = None,
            gsheet_key: str = None,
    ):
        """
        Only one input is required. If both are given, the expected logic is to:
        - open an existing sheet with input ID
        - open an existing sheet with input Name
        - create a new sheet if sheet with input name or ID doesn't exist

        Args:
            gsheet_name: name of google sheet you want to access or create
            gsheet_key: ID of google sheet you want to access
        """
        self.gsheet_name = gsheet_name
        self.gsheet_key = gsheet_key

        gc = gspread.authorize(GoogleCredentials.get_application_default())

        if self.gsheet_key is not None:
            self.sh = gc.open_by_key(self.gsheet_key)
            print(f"Opened existing google worksheet KEY: {self.gsheet_key} ...")
        else:
            try:
                self.sh = gc.open(self.gsheet_name)
                print(f"Opened existing google worksheet NAME: {self.gsheet_name} ...")
            except SpreadsheetNotFound:
                print(f"Created google worksheet: {self.gsheet_name} ...")
                self.sh = gc.create(self.gsheet_name)

    def get_or_create_worksheet(
            self,
            worksheet_name: str,
    ):
        """
        Given a worksheet_name, return the sheet if it exists or create it if it doesn't.

        Args:
            worksheet_name:

        Returns: worksheet object
        """
        try:
            wsh = self.sh.worksheet(worksheet_name)
            print(f"  Opening existing worksheet: {worksheet_name} ...")
        except WorksheetNotFound:
            print(f"  Creating NEW worksheet: {worksheet_name} ...")
            wsh = self.sh.add_worksheet(
                title=worksheet_name,
                rows=2,
                cols=2
            )
        return wsh

    def get_worksheet_as_df(
            self,
            worksheet_name: str,
    ) -> pd.DataFrame:
        """
        Given a worksheet name, get values as dataframe.
        If worksheet doesn't exist, raise error.

        Args:
            worksheet_name:

        Returns:
            pandas dataframe
        """
        wsh = self.sh.worksheet(worksheet_name)
        return pd.DataFrame(wsh.get_all_records())

    def update_worksheet_with_df(
            self,
            worksheet_name: str,
            df: pd.DataFrame,
            clear_before_update: bool = False,
    ):
        """
        Update worksheet with dataframe.

        NOTE: this may delete existing worksheet contents! We need to clear because
        it's possible that the new dataframe has fewer rows or columns than the previous df
        which would create weird artifacts (extra rows or extra columns).

        Args:
            worksheet_name:
            df:
            clear_before_update:

        Returns:

        """
        wsh = self.get_or_create_worksheet(worksheet_name)

        if clear_before_update:
            print(f"Clearing worksheet...")
            wsh.clear()

        return wsh.update(
            [df.columns.values.tolist()] +
            df.fillna('').values.tolist()
        )


#
# ~ fin
#
