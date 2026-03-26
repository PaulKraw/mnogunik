
import gspread

SPREADSHEET_ID = "1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ"
CREDENTIALS_FILE = 'config/credentials.json'

def get_client():
    print('start get_client')
    import json
    with open(CREDENTIALS_FILE) as f:
        creds = json.load(f)
    return gspread.service_account_from_dict(creds)

def get_sheet(client, sheet_name):
    # открываем таблицу по ключу
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=sheet_name, rows=10, cols=5)
    return worksheet
