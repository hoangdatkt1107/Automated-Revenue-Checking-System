import os
import datetime
import json
import pandas as pd
import gspread
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
from company_rename import rename_company
from read_data import read_data_company
from sqlalchemy import create_engine
from input_date import *


AUTH_KEY_PATH = os.path.join(os.getcwd(), "auth_key.json")
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

def get_drive_service():
    creds = ServiceAccountCredentials.from_json_keyfile_name(AUTH_KEY_PATH, SCOPES)
    return build('drive', 'v3', credentials=creds)

def get_sheets_service():
    creds = ServiceAccountCredentials.from_json_keyfile_name(AUTH_KEY_PATH, SCOPES)
    return gspread.authorize(creds)

# def get_previous_month_folder():
#     today = datetime.date.today()
#     first_of_this_month = today.replace(day=1)
#     first_of_last_month = first_of_this_month - datetime.timedelta(days=1)
#     return first_of_last_month.strftime("%Y-%m")

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

def read_google_sheets(sheet_id, sheet_name):
    sheets_service = get_sheets_service()
    sheet = sheets_service.open_by_key(sheet_id).worksheet(sheet_name)
    data = sheet.get_all_values()
    df = pd.DataFrame(data)
    return df

def read_excel_from_drive(drive_service, file_id):
    """Tải file Excel từ Google Drive và đọc bằng Pandas."""
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_stream.seek(0)
    df = pd.read_excel(file_stream, engine="openpyxl")  
    return df

def take_header(non_paylater):
    """
    Xác định hàng header trong DataFrame:
    - Nếu header đã ở dòng 0, kiểm tra xem đúng cột chưa.
    - Nếu chưa, tìm hàng chứa 'company', 'ac_employee_id', 'ac_revenue' rồi đặt làm header.
    - Nếu không tìm thấy, báo lỗi.
    """
    expected_columns = ["ac_company", "ac_employee_id", "ac_revenue"]

    # Kiểm tra nếu header đã đúng vị trí nhưng tên cột sai
    current_columns = [col.lower().strip() for col in non_paylater.columns]
    if all(col in current_columns for col in expected_columns):
        print("✅ Header đã ở đúng vị trí, không cần thay đổi.")
        return non_paylater

    checked_rows = []  
    header_index = None

    for i, row in non_paylater.iterrows():
        row_text = [str(cell).strip().lower() for cell in row]  

        # Kiểm tra nếu hàng này chứa đủ cả 3 từ khóa
        if all(keyword in row_text for keyword in expected_columns):
            header_index = i
            break  
        checked_rows.append(" | ".join(row_text))  

    # Nếu không tìm thấy header, báo lỗi & in các hàng đã kiểm tra
    if header_index is None:
        print("❌ Không tìm thấy hàng header phù hợp!")
        print("🔍 Các hàng đã kiểm tra:")
        for idx, row_text in enumerate(checked_rows):
            print(f"   - Dòng {idx}: {row_text}")
        return None

    # Nếu header nằm ở vị trí khác, cắt DataFrame từ đó
    print(f"Header được tìm thấy ở dòng {header_index}, cập nhật lại...")
    non_paylater = non_paylater.iloc[header_index:].reset_index(drop=True)
    non_paylater.columns = non_paylater.iloc[0]  
    non_paylater = non_paylater[1:].reset_index(drop=True)  

    return non_paylater

def find_tenant_suggestions_paylater(df, short_names):
    """
    Tìm tenant_code và created_date khi 'name' hoặc 'code' chứa bất kỳ giá trị nào trong danh sách short_names.
    """
    if isinstance(short_names, str):
        short_names = [short_names]  

    found_any = False  
    for short_name in short_names:
        matching_rows = df[
            df['name'].str.contains(short_name, case=False, na=False) |
            df['code'].str.contains(short_name, case=False, na=False)
        ]
        if matching_rows.empty:
            print(f"❌ Không tìm thấy tenant nào chứa '{short_name}'.")
        else:
            found_any = True  
            for _, row in matching_rows.iterrows():
                print(f"""🔍 Gợi ý: '{short_name}' có thể là: 
                      🔸 Tenant: '{row['tenant_code']}' 
                      🔸 Tenant_id: '{row['tenant_id']}',
                      🔸 Company_code: là '{row['code']}',
                      🔸 Tên đầy đủ là: '{row['name']} 
                      🔸 Ngày đầu rollout là {row['created_date']}
                      ➡️ Vui lòng thêm company vào function: rename_company trong file read_data.py rồi chạy lại nhé! """)

    if not found_any:
        print("""⚠️ Không tìm thấy tenant nào cho bất kỳ giá trị nào trong danh sách!
                 ➡️ Vui lòng thêm company vào function: rename_company trong file read_data.py rồi chạy lại nhé!""")

def validate_company_data_paylater(non_paylater):
    company_detail = read_data_company()
    missing_company = non_paylater[non_paylater['accountant_company'].isna()]

    if not missing_company.empty:
        for _, row in missing_company.iterrows():
            short_name_value = row['ac_company']  
            print(f"❌ Lỗi: Không tìm được thông tin của công ty '{short_name_value}'!")
        # Đưa ra gợi ý short name không match được với bất kì giá trị nào
            find_tenant_suggestions_paylater(company_detail,row['ac_company','ac_employee_id','ac_revenue'])

        raise ValueError("🚨 Lỗi: Có giá trị thiếu trong cột 'ac_company'. Dừng chương trình!")
    non_paylater = non_paylater[['accountant_company','ac_employee_id','ac_revenue']]
    print("✅ Dữ liệu hợp lệ, không có công ty nào bị thiếu thông tin.")
    return non_paylater

def fill_missing_value_paylater(df, columns, fill_value=0):
    """
    Hàm thay thế các giá trị NaN trong danh sách cột được chỉ định bằng một giá trị cụ thể.
    Tham số:
    - df (pd.DataFrame): DataFrame cần xử lý.
    - columns (list): Danh sách các cột cần thay thế NaN.
    - fill_value (any, mặc định = 0): Giá trị thay thế cho NaN.
    """
    for i in columns:
        if i in df.columns:
            df[i] = df[i].fillna(fill_value)
            print(f"✅ Đã thay thế các giá trị Null trong cột {columns} bằng 0")
        else:
            raise ValueError(f"❌ Không tìm thấy cột {columns}")
    
    return df

def take_paylater():
    drive_service = get_drive_service()
    
    previous_month_folder = get_previous_first_date()
    
    main_folder_id = "1HLvrHCouXZrKx9yiHQCia97OaSIqsH9H"

    month_folder_id, _ = find_item_in_folder(drive_service, main_folder_id, previous_month_folder, mime_type="application/vnd.google-apps.folder")
    if not month_folder_id:
        return
    
    accountant_folder_id, _ = find_item_in_folder(drive_service, month_folder_id, "accountant", mime_type="application/vnd.google-apps.folder")
    if not accountant_folder_id:
        return

    # Tìm thư mục "non_paylater_features" bên trong "accountant"
    non_paylater_folder_id, _ = find_item_in_folder(drive_service, accountant_folder_id, "paylater_features", mime_type="application/vnd.google-apps.folder")
    if not non_paylater_folder_id:
        return

    # Tìm file Google Sheets hoặc Excel trong "non_paylater_features"
    target_file_name = "Paylater Revenue"
    sheet_id, file_type = find_item_in_folder(drive_service, non_paylater_folder_id, target_file_name)

    if not sheet_id:
        return

    print(f"📂 Đã tìm thấy file: {target_file_name} (ID: {sheet_id})")

   # Kiểm tra loại file và đọc dữ liệu
    if file_type == "application/vnd.google-apps.spreadsheet":
        df = read_google_sheets(sheet_id, "Sheet1")  
    else:
        df = read_excel_from_drive(drive_service, sheet_id)  
    
    df = take_header(df) 
    df = df[df['ac_company'].notnull()]
    df['accountant_company'] = df['ac_company'].apply(rename_company)
    df = validate_company_data_paylater(df)

    columns_to_fill= ['accountant_company','ac_employee_id','ac_revenue']
    df = fill_missing_value_paylater(df,columns_to_fill)
    
    print("📊 Dữ liệu từ file:")

    return df



