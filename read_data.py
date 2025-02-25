import pandas as pd
from database import connect_to_postgres
import datetime
from input_date import *

# def get_previous_first_date():
    # today = datetime.date.today()
    # first_of_this_month = today.replace(day=1)  
    # first_of_last_month = first_of_this_month - datetime.timedelta(days=1)  
    # first_day_last_month = first_of_last_month.replace(day=1)  
    # first_day_last_month = first_day_last_month.strftime("%Y-%m-%d")
    # return first_day_last_month

# first_day_last_month = get_previous_first_date()
# def get_previous_first_date():
#     result = '2024-11-01'
#     result = pd.to_datetime(result, format='%Y-%m-%d')
#     result = result.date()
#     return result

# def get_input_date():
#     result = input('Hãy nhập ngày đầu tháng nhé, Format là YYYY-MM-DD 😘')
#     result = pd.to_datetime(result, format='%Y-%m-%d')
#     result = result.date()
#     return result

# def automate_fill_date():
#     today = datetime.date.today()
#     first_of_this_month = today.replace(day=1)  
#     first_of_last_month = first_of_this_month - datetime.timedelta(days=1)  
#     first_day_last_month = first_of_last_month.replace(day=1) 
#     return first_day_last_month

# def get_previous_first_date():
#    yes_no = input('Bạn có muốn nhập ngày ko hay tôi sẽ tự động nhập hộ bạn YES/NO').strip().lower()
#    if yes_no == 'yes':
#       result = get_input_date()
#    elif yes_no == 'no':
#        result = automate_fill_date()
#    else:
#        print('Sai Format, Hãy nhập lại1')
#        raise ValueError
#    return result


def read_data_ewa() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"""With a as (SELECT table_data.loan_id,
            table_data.date,
            table_data.employee_id,
            table_data.type,
            table_data.gross_revenue,
            table_data.deduction as transaction_discount,
            table_data.cashback_value+table_data.cashback_voucher_value as voucher_discount,
            ((table_data.deduction + table_data.cashback_value) + table_data.cashback_voucher_value) AS deduction_promotion,
            table_data.gross_revenue- table_data.deduction as accountant_revenue,
            (table_data.gross_revenue - ((table_data.deduction + table_data.cashback_value) + table_data.cashback_voucher_value)) AS net_revenue,
            table_data.disbursement,
            table_data.employment_id,
            ''::text AS ref_id,
            NULL::text AS reff_id,
            table_data.loan_id AS parent_id,
            table_data.disbursement AS loan_value,
            table_data.disbursement AS principal
        FROM ( SELECT l.id AS loan_id,
                    (work_period_entries.expected_end_date - '1 day'::interval) AS date,
                    e.employee_id,
                    l.type,
                        CASE
                            WHEN (((p.code)::text = ANY (ARRAY[('NGUYENKIM'::character varying)::text, ('CRV'::character varying)::text, ('GO'::character varying)::text, ('BIPBIP'::character varying)::text, ('TOPS'::character varying)::text, ('PROPERTY'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.percentage_fee)
                            WHEN (((p.code)::text = ANY (ARRAY[('HYPERMARKET'::character varying)::text, ('CRVHO'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.percentage_fee)
                            ELSE (l.original_user_fee + l.original_employer_fee)
                        END AS gross_revenue,
                        CASE
                            WHEN (((p.code)::text = ANY (ARRAY[('NGUYENKIM'::character varying)::text, ('CRV'::character varying)::text, ('GO'::character varying)::text, ('BIPBIP'::character varying)::text, ('TOPS'::character varying)::text, ('PROPERTY'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.fee_discount)
                            WHEN (((p.code)::text = ANY (ARRAY[('HYPERMARKET'::character varying)::text, ('CRVHO'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.fee_discount)
                            ELSE ((l.original_user_fee + l.original_employer_fee) - (l.user_fee + l.employer_fee))
                        END AS deduction,
                        CASE
                            WHEN (((p1.source_type)::text = 'Cashback'::text) AND ((p1.status)::text = 'Success'::text)) THEN p1.amount_value
                            ELSE (0)::numeric
                        END AS cashback_value,
                        CASE
                            WHEN (((p2.source_type)::text = 'CashbackVoucher'::text) AND ((p2.status)::text = 'Success'::text)) THEN p2.amount_value
                            ELSE (0)::numeric
                        END AS cashback_voucher_value,
                        CASE
                            WHEN (p.sales_type = 'tech_service'::text) THEN (0)::numeric
                            ELSE l.actually_received_value
                        END AS disbursement,
                    l.employment_id,
                    l.amount_value
                FROM ((((((operations.loan_applications l
                    JOIN operations.employments e ON ((l.employment_id = e.id)))
                    JOIN operations.projects_investors p ON ((e.project_id = p.id)))
                    LEFT JOIN ewa_production_tables.payment_details p0 ON ((l.id = (p0.ref_id)::bigint)))
                    LEFT JOIN ewa_production_tables.payment_details p1 ON ((((l.cashback_id)::bigint = (p1.ref_id)::bigint) AND (p0.payment_id = p1.payment_id))))
                    LEFT JOIN ewa_production_tables.payment_details p2 ON ((((l.cashback_voucher_id)::bigint = (p2.ref_id)::bigint) AND (p0.payment_id = p2.payment_id))))
                    JOIN operations.projects ON (((l.project_id = projects.id) AND ((date_trunc('month'::text, (l.created_date AT TIME ZONE 'Asia/Ho_Chi_Minh'::text)) >= projects.from_date)
                    AND (date_trunc('month'::text, (l.created_date AT TIME ZONE 'Asia/Ho_Chi_Minh'::text)) < projects.to_date))))
                    JOIN ewa_production_tables.work_period_entries ON l.work_period_entry_id = work_period_entries.id
                    )
                WHERE ((1 = 1) AND ((l.type)::text = 'Advance'::text) AND ((l.status)::text = 'Disbursed'::text))) table_data)
                Select date_trunc('month',a.date),user_profiles_view.company,count(loan_id) as transactions, sum(accountant_revenue) + sum(transaction_discount) as gross_revenue, sum(transaction_discount) as transaction_discount,
                    sum(accountant_revenue) as accountant_revenue, sum(voucher_discount) as voucher_discount, sum(accountant_revenue) - sum(voucher_discount) as net_revenue
                from a
                join l1_tables.user_profiles_view on user_profiles_view.employment_id = a.employment_id
                where 1 = 1
                and date_trunc('month',a.date) = '{first_day_last_month}'
                group by date_trunc('month',a.date),user_profiles_view.company"""
    ewa = pd.read_sql(query, connection)
    print('Got data from ewa')
    return ewa

def read_data_qr_transfer() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"""With a as (SELECT table_data.loan_id,
    table_data.date,
    table_data.employee_id,
    table_data.type,
    table_data.gross_revenue,
    table_data.deduction as transaction_discount,
    table_data.cashback_value+table_data.cashback_voucher_value as voucher_discount,
    ((table_data.deduction + table_data.cashback_value) + table_data.cashback_voucher_value) AS deduction_promotion,
    table_data.gross_revenue- table_data.deduction as accountant_revenue,
    (table_data.gross_revenue - ((table_data.deduction + table_data.cashback_value) + table_data.cashback_voucher_value)) AS net_revenue,
    table_data.disbursement,
    table_data.employment_id,
    ''::text AS ref_id,
    NULL::text AS reff_id,
    table_data.loan_id AS parent_id,
    table_data.disbursement AS loan_value,
    table_data.disbursement AS principal
   FROM ( SELECT l.id AS loan_id,
            (work_period_entries.expected_end_date - '1 day'::interval) AS date,
            e.employee_id,
            l.type,
                CASE
                    WHEN (((p.code)::text = ANY (ARRAY[('NGUYENKIM'::character varying)::text, ('CRV'::character varying)::text, ('GO'::character varying)::text, ('BIPBIP'::character varying)::text, ('TOPS'::character varying)::text, ('PROPERTY'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.percentage_fee)
                    WHEN (((p.code)::text = ANY (ARRAY[('HYPERMARKET'::character varying)::text, ('CRVHO'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.percentage_fee)
                    ELSE (l.original_user_fee + l.original_employer_fee)
                END AS gross_revenue,
                CASE
                    WHEN (((p.code)::text = ANY (ARRAY[('NGUYENKIM'::character varying)::text, ('CRV'::character varying)::text, ('GO'::character varying)::text, ('BIPBIP'::character varying)::text, ('TOPS'::character varying)::text, ('PROPERTY'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.fee_discount)
                    WHEN (((p.code)::text = ANY (ARRAY[('HYPERMARKET'::character varying)::text, ('CRVHO'::character varying)::text])) AND (l.created_date >= (projects.from_date)::timestamp with time zone) AND (l.created_date <= (projects.to_date)::timestamp with time zone)) THEN (l.amount_value * projects.fee_discount)
                    ELSE ((l.original_user_fee + l.original_employer_fee) - (l.user_fee + l.employer_fee))
                END AS deduction,
                CASE
                    WHEN (((p1.source_type)::text = 'Cashback'::text) AND ((p1.status)::text = 'Success'::text)) THEN p1.amount_value
                    ELSE (0)::numeric
                END AS cashback_value,
                CASE
                    WHEN (((p2.source_type)::text = 'CashbackVoucher'::text) AND ((p2.status)::text = 'Success'::text)) THEN p2.amount_value
                    ELSE (0)::numeric
                END AS cashback_voucher_value,
                CASE
                    WHEN (p.sales_type = 'tech_service'::text) THEN (0)::numeric
                    ELSE l.actually_received_value
                END AS disbursement,
            l.employment_id,
            l.amount_value
           FROM ((((((operations.loan_applications l
             JOIN operations.employments e ON ((l.employment_id = e.id)))
             JOIN operations.projects_investors p ON ((e.project_id = p.id)))
             LEFT JOIN ewa_production_tables.payment_details p0 ON ((l.id = (p0.ref_id)::bigint)))
             LEFT JOIN ewa_production_tables.payment_details p1 ON ((((l.cashback_id)::bigint = (p1.ref_id)::bigint) AND (p0.payment_id = p1.payment_id))))
             LEFT JOIN ewa_production_tables.payment_details p2 ON ((((l.cashback_voucher_id)::bigint = (p2.ref_id)::bigint) AND (p0.payment_id = p2.payment_id))))
             JOIN operations.projects ON (((l.project_id = projects.id) AND ((date_trunc('month'::text, (l.created_date AT TIME ZONE 'Asia/Ho_Chi_Minh'::text)) >= projects.from_date)
             AND (date_trunc('month'::text, (l.created_date AT TIME ZONE 'Asia/Ho_Chi_Minh'::text)) < projects.to_date))))
             JOIN ewa_production_tables.work_period_entries ON l.work_period_entry_id = work_period_entries.id
             )
          WHERE ((1 = 1) AND ((l.type)::text in ('Transfer','QrTransfer')) AND ((l.status)::text = 'Disbursed'::text))) table_data)
          Select date_trunc('month',a.date),user_profiles_view.company,count(loan_id) as transactions, sum(accountant_revenue) + sum(transaction_discount) as gross_revenue, sum(transaction_discount) as transaction_discount,
            sum(accountant_revenue) as accountant_revenue, sum(voucher_discount) as voucher_discount, sum(accountant_revenue) - sum(voucher_discount) as net_revenue
          from a
          join l1_tables.user_profiles_view on user_profiles_view.employment_id = a.employment_id
          where  date_trunc('month',a.date) = '{first_day_last_month}'
          group by date_trunc('month',a.date),user_profiles_view.company"""
    qr_transfer = pd.read_sql(query, connection)
    print('Got data from qr_transfer')
    return qr_transfer

def read_data_paylater() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"""Select date_trunc('month',materialized_accountant_spending_transactions.date),company, count(loan_id) as trans, sum(gross_revenue) as gross_revenue,sum(deduction_promotion) as deduction_promotion,sum(net_revenue) as net_revenue 
            from vui_app_report.materialized_accountant_spending_transactions 
            where type = 'Paylater' 
            and date_trunc('month',materialized_accountant_spending_transactions.date) = '{first_day_last_month}'
            group by date_trunc('month',materialized_accountant_spending_transactions.date),company"""
    paylater = pd.read_sql(query, connection)
    print('Got data from paylater')
    return paylater

def read_data_vas() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"""With a as (Select (work_period_entries.expected_end_date - '1 day'::interval) AS end_date,vas_transactions.* from vui_app_report.vas_transactions
                JOIN operations.loan_applications ON vas_transactions.loan_id = loan_applications.id
                            JOIN ewa_production_tables.work_period_entries ON loan_applications.work_period_entry_id = work_period_entries.id
                            )
                Select date_trunc('month',end_date),company,count(distinct loan_id) as loans,sum(gross_revenue) as gross_revenue,sum(deduction_promotion) as deduction_promotion,sum(net_revenue) as net_revenue from a
                join l1_tables.user_profiles_view on user_profiles_view.employment_id = a.employment_id
                where date_trunc('month',end_date) = '{first_day_last_month}'
                group by date_trunc('month',end_date),company"""
    vas = pd.read_sql(query, connection)
    print('Got data from vas')
    return vas

def read_data_subscription() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"""Select total_subscription.* from vui_app_report.total_subscription 
               where date_trunc('month',total_subscription.date) = '{first_day_last_month}'"""
    subscription = pd.read_sql(query, connection)
    print('Got data from subscription')
    return subscription

def read_data_company() -> pd.DataFrame:

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = """select tenants.code as tenant_code, tenant_id, companies.created_date, name, companies.code
                from ewa_production_tables.companies
                join ewa_production_tables.tenants on companies.tenant_id = tenants.id"""
    company = pd.read_sql(query, connection)
    return company

# *** This part for importing paylater data ***

def read_data_user_profiles_view() -> pd.DataFrame:
    connection = connect_to_postgres()
    if connection is None:
        return None

    query = "select * from l1_tables.user_profiles_view"
    user_profiles_view = pd.read_sql(query, connection)
    print('Got data from user_profiles_view')
    return user_profiles_view

def read_data_user_payale() -> pd.DataFrame:
    connection = connect_to_postgres()
    if connection is None:
        return None

    query = "select * from paylater.user_payable"
    user_payable = pd.read_sql(query, connection)
    print('Got data from user_payable')
    return user_payable

def read_data_user_pay_report() -> pd.DataFrame:
    connection = connect_to_postgres()
    if connection is None:
        return None

    query = "select * from paylater.user_pay_report"
    user_pay_report = pd.read_sql(query, connection)
    print('Got data from user_pay_report')
    return user_pay_report

def read_data_user_pay_history() -> pd.DataFrame:

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = "select * from paylater.user_pay_history"
    user_pay_history = pd.read_sql(query, connection)
    print('Got data from user_pay_history')
    return user_pay_history

def read_data_installment_loan() -> pd.DataFrame:
    connection = connect_to_postgres()
    if connection is None:
        return None

    query = "select * from paylater.installment_loan"
    installment_loan = pd.read_sql(query, connection)
    print('Got data from installment_loan')
    return installment_loan

def read_data_accountant_spending_transactions() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"select * from vui_app_report.accountant_spending_transactions where date_trunc('month',date) = '{first_day_last_month}' and type = 'Paylater'"
    accountant_spending_transactions = pd.read_sql(query, connection)
    print('Got data from accountant_spending_transactions')
    return accountant_spending_transactions

def read_data_ar_tracking() -> pd.DataFrame:
    first_day_last_month = get_previous_first_date()

    connection = connect_to_postgres()
    if connection is None:
        return None

    query = f"""select employment_id, total_debt_amount, loan_status
                from ewa_prod_reconcile.b2c_loan_accounts
                where true
                and total_paylater_amount != 0"""
    ar_tracking = pd.read_sql(query, connection)
    print('Got data from ar_tracking')
    connection.close()
    print('connection_closed')
    return ar_tracking







