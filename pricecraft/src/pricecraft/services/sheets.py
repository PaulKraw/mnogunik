# services/sheets.py
import gspread
from google.oauth2.service_account import Credentials
import os, numpy as np, math, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_FILE = os.path.join(ROOT, 'config', 'credentials.json')

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_client():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def open_sheet(spreadsheet_id):
    client = get_client()
    return client.open_by_key(spreadsheet_id)

def clean_df_for_sheet(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    # replace inf with nan, then fillna safely
    df2 = df2.replace([np.inf, -np.inf], np.nan)
    df2 = df2.fillna("")
    # convert numpy scalars to python scalars
    def to_native(x):
        try:
            if hasattr(x, 'item'):
                return x.item()
        except:
            pass
        # handle non-finite floats
        if isinstance(x, float) and not math.isfinite(x):
            return ""
        return x
    df2 = df2.applymap(to_native)
    # finally cast to strings so json safe
    return df2.astype(str)

def update_all_from_df(spreadsheet_id, sheet_name_or_index, df):
    """
    spreadsheet_id: Google sheet id
    sheet_name_or_index: either sheet title (str) or index (int)
    df: pandas DataFrame
    """
    sh = open_sheet(spreadsheet_id)
    if isinstance(sheet_name_or_index, int):
        worksheet = sh.get_worksheet(sheet_name_or_index)
    else:
        worksheet = sh.worksheet(sheet_name_or_index)

    # clean
    df_clean = clean_df_for_sheet(df).reset_index(drop=True)

    rows = [df_clean.columns.tolist()] + df_clean.values.tolist()

    # clear then update
    worksheet.clear()
    # A1 update
    worksheet.update('A1', rows, value_input_option="USER_ENTERED")
