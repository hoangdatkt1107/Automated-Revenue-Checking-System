import pandas as pd
from dateutil.relativedelta import relativedelta
from import_non_paylater import  take_non_paylater
from import_paylater import take_paylater
from system_data_processing import process_revenue_data_ewa_qr_transfer
from read_data import *
from company_rename import *
from helper_function import *
from input_date import *


def calculate_differences_ewa_transfer() -> pd.DataFrame:
    """
    Hợp nhất dữ liệu từ system_check và non_paylater_data, sau đó tính toán chênh lệch doanh thu và số lượng đơn.

    Tham số:
    - system_check (pd.DataFrame): DataFrame chứa dữ liệu từ hệ thống EWA & QR Transfer.
    - non_paylater_data (pd.DataFrame): DataFrame chứa dữ liệu từ hệ thống Non-Paylater.

    Trả về:
    - pd.DataFrame: DataFrame chứa dữ liệu đã hợp nhất và lọc theo điều kiện chênh lệch doanh thu.
    """
    # from import_non_paylater import  take_non_paylater
    # from system_data_processing import process_revenue_data_ewa_qr_transfer

    system_check = process_revenue_data_ewa_qr_transfer()
    non_paylater_data = take_non_paylater()

    # Hợp nhất dữ liệu dựa trên tên công ty
    merged_df = system_check.merge(non_paylater_data, left_on='ewa_qr_transfer_company', right_on='company', how='outer')

    # Tính chênh lệch doanh thu và số lượng đơn
    merged_df['revenue_diff'] = merged_df['accountant_revenue'] - merged_df['ewa_qr_transfer_system_revenue']
    merged_df['transactions_diff'] = merged_df['Số lượng đơn'] - merged_df['ewa_qr_transfer_system_transactions']

    # Xử lý tên công ty (lấy giá trị không null từ một trong hai cột)
    merged_df['ewa_qr_transfer_company'] = merged_df['ewa_qr_transfer_company'].fillna(merged_df['company'])

    # Chọn các cột cần thiết
    selected_columns = [
        'ewa_qr_transfer_company', 'ewa_qr_transfer_system_transactions', 'ewa_qr_transfer_system_revenue',
        'Số lượng đơn', 'accountant_revenue', 'transactions_diff', 'revenue_diff'
    ]
    merged_df = merged_df[selected_columns]

    # Lọc các dòng có chênh lệch doanh thu lớn hơn ±10,000
    filtered_df = merged_df[(merged_df['revenue_diff'].abs() > 10000)]

    return filtered_df

# 🔹 Ví dụ sử dụng:
# filtered_result = merge_and_calculate_differences(system_check, non_paylater_data)
# print(filtered_result)


def calculate_differences_subscription() -> pd.DataFrame:
    """
    Xử lý và kiểm tra dữ liệu doanh thu từ hệ thống Subscription và Non-Paylater.

    Trả về:
    - pd.DataFrame: DataFrame chứa chênh lệch doanh thu và chiết khấu thương mại giữa hai hệ thống.
    """

    # Đọc dữ liệu từ hệ thống
    subscription = read_data_subscription()
    non_paylater_data = take_non_paylater()

    # Lọc dữ liệu: Chỉ lấy các dòng có "Phí subscription" hoặc "Chiết khấu thương mại" khác 0
    non_paylater_data = non_paylater_data[
        (non_paylater_data['Phí subcription'] != 0) | (non_paylater_data['Chiết khấu thương mại'] != 0)]

    # Chọn cột cần thiết từ Non-Paylater & Subscription
    accountant_subscription = non_paylater_data[['company', 'Phí subcription', 'Chiết khấu thương mại']]
    system_subscription = subscription[['company', 'gross_revenue', 'deduction_promotion']]

    # Đổi tên cột để tránh trùng lặp khi merge
    accountant_subscription = accountant_subscription.rename(columns={'company': 'accountant_company'})
    system_subscription = system_subscription.rename(columns={'company': 'system_company'})

    # Hợp nhất dữ liệu từ hai nguồn theo tên công ty
    subscription_checking = system_subscription.merge(
        accountant_subscription, left_on='system_company', right_on='accountant_company', how='outer')

    # Xử lý tên công ty chung (ghép dữ liệu từ cả hai hệ thống)
    subscription_checking['subcription_company'] = subscription_checking['system_company'].fillna(
        subscription_checking['accountant_company'])

    # Thay thế tất cả giá trị NaN bằng 0
    subscription_checking.fillna(0, inplace=True)

    # Tính toán chênh lệch doanh thu & chiết khấu thương mại
    subscription_checking['gross_revenue_diff'] = (
        subscription_checking['Phí subcription'] - subscription_checking['gross_revenue'])
    subscription_checking['promotion_diff'] = (
        subscription_checking['Chiết khấu thương mại'] - subscription_checking['deduction_promotion'])

    # Chọn cột quan trọng cần hiển thị
    final_columns = ['subcription_company','Phí subcription','Chiết khấu thương mại','gross_revenue', 'deduction_promotion','gross_revenue_diff', 'promotion_diff']
    subscription_checking = subscription_checking[final_columns]

    return subscription_checking

# 🔹 Ví dụ sử dụng:
# subscription_result = process_subscription_data()
# print(subscription_result)

def calculate_differences_coincide_paylater():
    """
    Xử lý dữ liệu kế toán và kiểm tra thiếu sót trong employment_id.
    
    Tham số:
    - ac: DataFrame chứa thông tin nhân viên và công ty kế toán.
    - user_profiles_view: DataFrame chứa thông tin nhân viên từ hệ thống.
    - accountant_spending_transactions: DataFrame chứa giao dịch chi tiêu.
    - rename_function: Hàm đổi tên công ty (vd: rename_company_crv).
    
    Returns:
        DataFrame đã xử lý với dòng tổng hợp revenue_diff.
    """
    
    user_profiles_view = read_data_user_profiles_view()
    accountant_spending_transactions = read_data_accountant_spending_transactions() 
    ac = take_paylater()

    # Chuẩn hóa tên công ty trong cả hai bảng
    user_profiles_view['company'] = user_profiles_view['company'].apply(rename_company_crv)
    accountant_spending_transactions['company'] = accountant_spending_transactions['company'].apply(rename_company_crv)

    # Merge ac với user_profiles_view để lấy employment_id
    ac = ac.merge(user_profiles_view, left_on=('ac_employee_id', 'accountant_company'), 
                  right_on=('employee_id', 'company'), how='left')

    # Chỉ giữ lại các cột cần thiết
    ac = ac[['accountant_company', 'ac_employee_id', 'ac_revenue', 'employment_id']]

    # Kiểm tra nhân viên nào không có employment_id
    missing_employment_id = ac.loc[ac['employment_id'].isna(), ['ac_employee_id', 'accountant_company']]
    if not missing_employment_id.empty:
        for _, row in missing_employment_id.iterrows():
            print(f"❌ Lỗi: Nhân viên ID {row['ac_employee_id']} của công ty {row['accountant_company']} không có `employment_id`!")
        raise ValueError("🚨 Lỗi nghiêm trọng: Có nhân viên thiếu `employment_id`. Dừng chương trình!")
    
    ac = ac.rename(columns={'employment_id':'ac_employment_id'})

    # Gộp dữ liệu chi tiêu của kế toán theo employee_id
    accountant_spending_transactions = accountant_spending_transactions.groupby('employment_id').agg({
        'net_revenue': 'sum',
        'employee_id': 'first',
        'company': 'first',
        'date': 'first'
    }).reset_index()
    # Gộp dữ liệu ac theo ac_employee_id
    ac = ac.groupby('ac_employment_id').agg({
        'ac_employee_id':'first',
        'accountant_company': 'first',
        'ac_revenue': 'sum'
    }).reset_index()
    # Merge hai bảng theo employee_id
    amount_checking = ac.merge(accountant_spending_transactions, left_on='ac_employment_id', 
                               right_on='employment_id', how='inner')

    # Chỉ lấy các cột cần thiết
    amount_checking = amount_checking[['company','employment_id','employee_id', 'net_revenue', 'ac_revenue']]
    # Tính chênh lệch revenue
    amount_checking['revenue_diff'] = amount_checking['ac_revenue'] - amount_checking['net_revenue']
    # Thêm dòng tổng vào cuối bảng
    amount_checking = add_total_row_paylater(amount_checking, sum_column="revenue_diff")
    amount_checking['employment_id'] = amount_checking['employment_id'].astype(str)
    return amount_checking

def process_and_label_loan_status():
    """
    Xử lý dữ liệu amount_checking từ các nguồn khác nhau, chuẩn hóa công ty, merge dữ liệu, 
    kiểm tra trạng thái khoản vay, và thêm dòng tổng vào cuối bảng.
    
    Returns:
        DataFrame đã xử lý.
    """

    # Đọc dữ liệu từ các nguồn
    user_profiles_view = read_data_user_profiles_view()
    accountant_spending_transactions = read_data_accountant_spending_transactions()
    user_payable = read_data_user_payale()
    user_pay_report = read_data_user_pay_report()
    user_pay_history = read_data_user_pay_history()
    installment_loan = read_data_installment_loan()
    ar_tracking = read_data_ar_tracking()
    ac = take_paylater()

    # Chuẩn hóa tên công ty**
    for df in [user_profiles_view, accountant_spending_transactions]:
        df['company'] = df['company'].apply(rename_company_crv)

    # Merge ac với user_profiles_view để lấy employment_id
    ac = ac.merge(user_profiles_view, left_on=('ac_employee_id', 'accountant_company'), 
                  right_on=('employee_id', 'company'), how='left')
    ac = ac[['accountant_company', 'ac_employee_id', 'ac_revenue', 'employment_id']].rename(
        columns={'employment_id': 'ac_employment_id'}
    )

    # Kiểm tra thiếu employment_id
    missing_employment_id = ac.loc[ac['ac_employment_id'].isna(), ['ac_employee_id', 'accountant_company']]
    if not missing_employment_id.empty:
        for _, row in missing_employment_id.iterrows():
            print(f"❌ Lỗi: Nhân viên ID {row['ac_employee_id']} của công ty {row['accountant_company']} không có `employment_id`!")
        raise ValueError("🚨 Lỗi nghiêm trọng: Có nhân viên thiếu `employment_id`. Dừng chương trình!")

    # Gộp dữ liệu kế toán theo employee_id
    accountant_spending_transactions = accountant_spending_transactions.groupby('employment_id').agg({
        'net_revenue': 'sum',
        'employee_id': 'first',
        'company': 'first',
        'date': 'first'
    }).reset_index()

    ac = ac.groupby('ac_employment_id').agg({
        'ac_employee_id': 'first',
        'accountant_company': 'first',
        'ac_revenue': 'sum'
    }).reset_index()

    # Merge dữ liệu kế toán và AR tracking
    amount_checking = ac.merge(accountant_spending_transactions, left_on='ac_employment_id', right_on='employment_id', how='outer')
    amount_checking = amount_checking.loc[amount_checking['ac_employment_id'].isna(), ['date', 'employment_id', 'net_revenue', 'employee_id', 'company']]
    
    ar_tracking = ar_tracking.rename(columns={'employment_id': 'ar_employment_id'})
    amount_checking = amount_checking.merge(ar_tracking, left_on='employment_id', right_on='ar_employment_id', how='left').drop(columns=['ar_employment_id', 'total_debt_amount'])

    # Kiểm tra trạng thái khoản vay
    amount_checking['loan_status'] = amount_checking.apply(check_cs_status, axis=1)

    # Xử lý dữ liệu installment loan
    payable_report_merge = user_payable.merge(user_pay_report, left_on='id', right_on='user_payable_id', how='inner')
    payable_report_merge = payable_report_merge[['installment_loan_id', 'status', 'amount_x', 'payable_date', 'type', 'created_date_y']].rename(
        columns={'amount_x': 'payable_amount', 'created_date_y': 'report_created_date'}
    )

    installment_payable_report_merge = payable_report_merge.merge(installment_loan, left_on='installment_loan_id', right_on='id', how='inner')
    installment_payable_report_merge = installment_payable_report_merge[
        ['installment_loan_id', 'employment_id', 'status_x', 'payable_amount', 'payable_date', 'type', 'report_created_date']
    ].rename(columns={'status_x': 'installment_status'})

    installment_payable_report_merge = installment_payable_report_merge[
        installment_payable_report_merge['type'] == 'Fee'
    ]
    installment_payable_report_merge['report_created_date'] = installment_payable_report_merge['report_created_date'].dt.date
    installment_payable_report_merge['system_status'] = installment_payable_report_merge.apply(check_system_status, axis=1)

    # Lọc dữ liệu theo tháng
    date1 = get_previous_first_date()
    revenue_date = date1 + relativedelta(months=1)
    installment_payable_report_merge['start_month'] = installment_payable_report_merge['payable_date'].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)
    installment_payable_report_merge = installment_payable_report_merge[installment_payable_report_merge['start_month'] == revenue_date]

    installment_payable_report_merge = installment_payable_report_merge.drop_duplicates(subset=['employment_id'], keep='first')

    amount_checking = amount_checking.merge(installment_payable_report_merge, left_on='employment_id', right_on='employment_id', how='left')
    amount_checking['date'] = amount_checking['date'].dt.date

    amount_checking = amount_checking[['company', 'employee_id', 'employment_id', 'date', 'net_revenue', 'loan_status', 'system_status']]

    # Xử lý dữ liệu payable_installment_merge
    payable_installment_merge = user_payable.merge(installment_loan, left_on='installment_loan_id', right_on='id', how='inner')
    payable_installment_merge = payable_installment_merge[['employment_id', 'type', 'payable_date']]
    
    payable_installment_merge['payable_date'] = payable_installment_merge['payable_date'].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)
    payable_installment_merge = payable_installment_merge.loc[
        (payable_installment_merge['payable_date'].notna()) & (payable_installment_merge['type'] == 'DailyInterest')
    ]
    
    payable_installment_merge = payable_installment_merge[payable_installment_merge['payable_date'] == revenue_date]
    payable_installment_merge = payable_installment_merge.rename(columns={'employment_id': 'payable_employment_id'})
    payable_installment_merge = payable_installment_merge.drop_duplicates(subset=['payable_employment_id'], keep='first')

    amount_checking = amount_checking.merge(payable_installment_merge, left_on='employment_id', right_on='payable_employment_id', how='left')
    amount_checking['final_status'] = amount_checking['loan_status'].combine_first(amount_checking['system_status']).combine_first(amount_checking['type']).fillna("None")
    amount_checking = amount_checking[['company','employee_id','date','net_revenue','final_status']]

    # Thêm dòng tổng
    amount_checking = add_total_row_paylater(amount_checking, sum_column='net_revenue')

    return amount_checking
