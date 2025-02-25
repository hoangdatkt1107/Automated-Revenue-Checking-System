import pandas as pd
import datetime
import os

def add_total_row_paylater(df: pd.DataFrame, sum_column: str, label: str = "TOTAL") -> pd.DataFrame:
    """
    Thêm một dòng tổng cộng vào cuối DataFrame, tính tổng cho một cột cụ thể.
    
    - df: DataFrame cần thêm dòng tổng.
    - sum_column: Tên cột cần tính tổng.
    - label: Giá trị trong cột đầu tiên để thể hiện dòng tổng (mặc định là "TOTAL").
    
    Returns:
        DataFrame mới với dòng tổng cộng.
    """
    total_value = df[sum_column].sum()

    # Tạo một dòng tổng hợp
    summary_row = pd.DataFrame({col: [""] for col in df.columns})  
    summary_row.iloc[0, 0] = label  
    summary_row[sum_column] = total_value  

    return pd.concat([df, summary_row], ignore_index=True)

def check_cs_status(row):
    if row['loan_status'] == 'PartialPaid':
        return 'CS_PartialPaid'
    elif row['loan_status'] == 'Pending':
        return 'CS_Pending'
    elif row['loan_status'] == 'WriteOff':
        return 'CS_WriteOff'
    elif row['loan_status'] == 'Processing':
        return 'CS_Processing'
    elif row['loan_status'] == 'Paid':
        return 'CS_Paid'
    elif row['loan_status'] == 'Closed':
        return 'CS_Closed'   
    else:
        return None

def check_system_status(row):
    if row['report_created_date'] > row['payable_date']:
        return 'system_done(late)'
    elif row['report_created_date'] <= row['payable_date']:
        return 'system_done(on_time)'
    else:
        return None

def delete_cache_file():
    """Xóa file cache.json sau khi chạy xong chương trình."""
    CACHE_FILE = "cache.json"  
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        print("🗑️ File 'cache.json' đã bị xóa sau khi chương trình kết thúc.")
    else:
        print("✅ Không có file 'cache.json' để xóa.")