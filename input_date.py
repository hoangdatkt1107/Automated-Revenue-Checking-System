import datetime
import pandas as pd
import json
import os

CACHE_FILE = "cache.json"  

def save_cache(date_value, user_choice=None):
    """ Lưu ngày và lựa chọn của người dùng vào file cache """
    data = {"date": date_value.strftime("%Y-%m-%d")}
    if user_choice:
        data["user_choice"] = user_choice  

    with open(CACHE_FILE, "w") as f:
        json.dump(data, f)

def load_cache():
    """ Đọc ngày và lựa chọn từ file cache nếu có """
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            try:
                data = json.load(f)
                date_value = pd.to_datetime(data["date"]).date()
                user_choice = data.get("user_choice", None)  
                return date_value, user_choice
            except Exception:
                return None, None
    return None, None

def delete_cache():
    """ Xóa file cache nếu tồn tại """
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        print("🗑️ Đã xóa ngày cũ trong cache!")

def get_input_date():
    """ Yêu cầu nhập ngày từ người dùng """
    result = input('📌 Hãy nhập ngày đầu tháng (YYYY-MM-DD): ').strip()
    try:
        result = pd.to_datetime(result, format='%Y-%m-%d').date()
        save_cache(result, user_choice="new")  
        return result
    except ValueError:
        print("❌ Sai Format! Nhập lại theo format YYYY-MM-DD.")
        return get_input_date()

def automate_fill_date():
    """ Lấy ngày đầu tháng trước tự động """
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    return (first_of_this_month - datetime.timedelta(days=1)).replace(day=1)

def get_previous_first_date():
    """ Lấy ngày từ cache hoặc yêu cầu nhập mới """
    cached_date, cached_choice = load_cache()

    if cached_date and cached_choice:
        print(f"📌 Sử dụng ngày từ cache: {cached_date} (Lựa chọn: {cached_choice.upper()})")
        return cached_date

    if cached_date:
        action = input(f'📌 Bạn đã nhập ngày trước đó là {cached_date}. Bạn muốn (NEW để nhập mới / DELETE để xóa cache / NO để dùng lại)? ').strip().lower()

        if action in ['new', 'delete', 'no']:
            save_cache(cached_date, action) 

        if action == 'new':
            return get_input_date() 
        elif action == 'delete':
            delete_cache()
            return get_input_date()  
        elif action == 'no':
            return cached_date  
        else:
            print("❌ Sai lựa chọn! Vui lòng nhập NEW, DELETE hoặc NO.")
            raise ValueError("Lựa chọn không hợp lệ!")

    yes_no = input('🤔 Bạn có muốn nhập ngày không? (YES/NO): ').strip().lower()

    if yes_no == 'yes':
        return get_input_date()
    elif yes_no == 'no':
        auto_date = automate_fill_date()
        save_cache(auto_date, "no")  
        return auto_date
    else:
        print("❌ Sai Format, nhập lại!")
        raise ValueError("Lựa chọn không hợp lệ!")