from comparasion_function import *
from  upload_file import upload_file_to_google_drive
def main_task():
    subscription = calculate_differences_subscription()
    # Tính toán và so sánh EWA + Qr_Transfer
    calculate_differences_ewa = calculate_differences_ewa_transfer()
    # Tính toán và so sánh dựa trên phí thu về so với net_revenue
    coincide_paylater = calculate_differences_coincide_paylater()
    # Tính toán và so sánh những user không có thông tin trong file kế toán
    checking_missing_employee = process_and_label_loan_status()
    # Upload
    upload_file_to_google_drive('subscription',subscription)
    upload_file_to_google_drive("ewa_qr_transfer",calculate_differences_ewa)
    upload_file_to_google_drive("coincide_paylater",coincide_paylater)
    upload_file_to_google_drive("checking_missing_employee",checking_missing_employee)
    delete_cache_file()
    print('hoàn thành chương trình!!!!!!')

if __name__ == "__main__":
    main_task()
    print("🛑 Exiting container...")
    exit(0)  # Thoát sau khi hoàn thành

