import pandas as pd
from read_data import *
from company_rename import rename_company_crv

def process_revenue_data_ewa_qr_transfer():
    """
    Hàm hợp nhất và xử lý dữ liệu doanh thu từ hệ thống EWA và QR Transfer.
    """
    
    ewa_revenue = read_data_ewa()
    qr_transfer_revenue = read_data_qr_transfer()
    # Chọn và đổi tên cột trong EWA
    ewa = ewa_revenue[['company', 'transactions', 'accountant_revenue']].rename(columns={
        'company': 'ewa_company',
        'transactions': 'ewa_transactions',
        'accountant_revenue': 'ewa_accountant_revenue'
    })

    # Chọn và đổi tên cột trong QR Transfer
    qr_transfer = qr_transfer_revenue[['company', 'transactions', 'accountant_revenue']].rename(columns={
        'company': 'qr_transfer_company',
        'transactions': 'qr_transfer_transactions',
        'accountant_revenue': 'qr_transfer_accountant_revenue'
    })

    # Hợp nhất dữ liệu
    system_check = ewa.merge(qr_transfer, left_on='ewa_company', right_on='qr_transfer_company', how='outer')

    # Xử lý tên công ty chung
    system_check['ewa_qr_transfer_company'] = system_check['ewa_company'].fillna(system_check['qr_transfer_company'])

    # Tính tổng giao dịch & doanh thu
    system_check['ewa_qr_transfer_system_transactions'] = system_check[['ewa_transactions', 'qr_transfer_transactions']].sum(axis=1, skipna=True)
    system_check['ewa_qr_transfer_system_revenue'] = system_check[['ewa_accountant_revenue', 'qr_transfer_accountant_revenue']].sum(axis=1, skipna=True)

    # Chọn các cột cần thiết
    system_check = system_check[['ewa_qr_transfer_company', 'ewa_qr_transfer_system_transactions', 'ewa_qr_transfer_system_revenue']]

    # Thay thế NaN bằng 0
    system_check.fillna({'ewa_qr_transfer_system_transactions': 0, 'ewa_qr_transfer_system_revenue': 0}, inplace=True)

    # Áp dụng hàm chuẩn hóa
    system_check['ewa_qr_transfer_company'] = system_check['ewa_qr_transfer_company'].apply(rename_company_crv)

    # Gộp theo `ewa_qr_transfer_company` và tính tổng
    system_check = system_check.groupby('ewa_qr_transfer_company', as_index=False).sum()

    return system_check

# 🔹 Ví dụ sử dụng:
# system_check_result = merge_and_process_revenue_data(ewa_revenue, qr_transfer_revenue)
# print(system_check_result)