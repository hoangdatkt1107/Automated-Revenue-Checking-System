import os
import pandas as pd
from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from input_date import *

# 🔹 Cấu hình Google Drive API
AUTH_KEY_PATH = os.path.join(os.getcwd(), "auth_key.json")
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

# 🔹 Xác thực Google Drive API
def get_drive_service():
    creds = ServiceAccountCredentials.from_json_keyfile_name(AUTH_KEY_PATH, SCOPES)
    return build('drive', 'v3', credentials=creds)

# 🔹 Tìm thư mục hoặc file trong Google Drive
def find_item_in_folder(drive_service, folder_id, item_name, mime_type=None):
    query = f"name contains '{item_name}' and '{folder_id}' in parents"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"

    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    items = results.get("files", [])
    
    if items:
        print(f"📂 Tìm thấy: {items[0]['name']} (ID: {items[0]['id']})")
        return items[0]['id'], items[0]['mimeType']
    else:
        print(f"❌ Không tìm thấy '{item_name}' trong thư mục ID {folder_id}!")
        return None, None

def upload_file_to_drive(file_path, folder_id=None, convert_to_gsheet=False):
    drive_service = get_drive_service()
    file_name = os.path.basename(file_path)
    file_metadata = {'name': file_name}

    if folder_id:
        file_metadata['parents'] = [folder_id]

    if convert_to_gsheet:
        file_metadata['mimeType'] = 'application/vnd.google-apps.spreadsheet'
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    else:
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    media = MediaFileUpload(file_path, mimetype=mime_type)
    uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    
    print(f"📂 File '{file_name}' đã được tải lên Drive (ID: {uploaded_file.get('id')})")
    return uploaded_file.get('id')

def save_and_upload_df(df, df_name="data", folder_id=None, convert_to_gsheet=False):
    """
    Lưu DataFrame và upload lên Google Drive với tên dựa trên biến.

    - df: DataFrame cần upload
    - df_name: Tên file dựa trên tên biến
    - folder_id: ID thư mục Google Drive
    - convert_to_gsheet: Nếu True, chuyển thành Google Sheets
    """
    file_name = f"{df_name}.xlsx"  
    df.to_excel(file_name, index=False, engine='openpyxl')

    file_id = upload_file_to_drive(file_name, folder_id, convert_to_gsheet)

    os.remove(file_name)
    
    return file_id

def upload_file_to_google_drive(df_name, df):
    """
    Upload một DataFrame lên thư mục `result` trong Google Drive.
    
    - df_name: Tên file (chuỗi)
    - df: DataFrame cần upload
    """
    drive_service = get_drive_service()
    previous_month_folder = get_previous_first_date()

    main_folder_id = "1HLvrHCouXZrKx9yiHQCia97OaSIqsH9H"

    month_folder_id, _ = find_item_in_folder(drive_service, main_folder_id, previous_month_folder, mime_type="application/vnd.google-apps.folder")
    if not month_folder_id:
        return

    result_folder_id, _ = find_item_in_folder(drive_service, month_folder_id, "result", mime_type="application/vnd.google-apps.folder")
    if not result_folder_id:
        return

    file_id = save_and_upload_df(df, df_name, result_folder_id, convert_to_gsheet=True)
    print(f"✅ File '{df_name}.xlsx' đã tải lên Google Drive (ID: {file_id})")