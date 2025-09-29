import numpy as np
import pandas as pd
import os

# Utils

def remove_last_newline(filename):
    with open(filename, 'rb+') as file:
        file.seek(-1, os.SEEK_END)
        if file.read(1) == b'\n':
            file.truncate(file.tell() - 1)

# Filename Importing

transactions_files = [f for f in os.listdir('./data/transactions') if f.startswith('transactions_') and f.endswith('.csv') and ('2024' in f or '2025' in f)]
transactions_items_files_2024 = [f for f in os.listdir('./data/transaction_items') if f.startswith('transaction_items_') and f.endswith('.csv') and '2024' in f]
transactions_items_files_2025 = [f for f in os.listdir('./data/transaction_items') if f.startswith('transaction_items_') and f.endswith('.csv') and '2025' in f]
users_files = [f for f in os.listdir('./data/users') if f.startswith('users_') and f.endswith('.csv')]
menu_items_file = './data/menu_items/menu_items.csv'
payment_methods_file = './data/payment_methods/payment_methods.csv'
stores_file = './data/stores/stores.csv'
voucher_file = './data/vouchers/vouchers.csv'

# Transactions Data Importing and Cleaning

transactions_sample = pd.read_csv('data/transactions/transactions_202401.csv', nrows=0)
transactions_keep_idx = [i for i in range(transactions_sample.shape[1]) if i not in {2, 3, 6}]

transactions = pd.concat([pd.read_csv('./data/transactions/' + f, usecols=transactions_keep_idx, low_memory=False) for f in transactions_files], ignore_index=True)

transactions["transaction_id"] = transactions["transaction_id"].astype("string").str.strip()
transactions["store_id"] = pd.to_numeric(transactions["store_id"], errors="coerce").astype("Int64")
transactions["user_id"] = pd.to_numeric(transactions["user_id"], errors="coerce").astype("Int64")
transactions["original_amount"] = pd.to_numeric(transactions["original_amount"], errors="coerce").astype("float64")
transactions["final_amount"]    = pd.to_numeric(transactions["final_amount"],    errors="coerce").astype("float64")
transactions["created_at"] = pd.to_datetime(transactions["created_at"], errors="coerce")


# Transactions Items Data Importing and Cleaning

transactions_items_2024 = pd.concat([pd.read_csv('./data/transaction_items/' + f, low_memory=False) for f in transactions_items_files_2024], ignore_index=True)
transactions_items_2025 = pd.concat([pd.read_csv('./data/transaction_items/' + f, low_memory=False) for f in transactions_items_files_2025], ignore_index=True)

transactions_items_2024["transaction_id"] = transactions_items_2024["transaction_id"].astype("string").str.strip()
transactions_items_2024["item_id"] = pd.to_numeric(transactions_items_2024["item_id"], errors="coerce").astype("Int64")
transactions_items_2024["quantity"] = pd.to_numeric(transactions_items_2024["quantity"], errors="coerce").astype("Int64")
transactions_items_2024["unit_price"] = pd.to_numeric(transactions_items_2024["unit_price"], errors="coerce").astype("float64")
transactions_items_2024["subtotal"]    = pd.to_numeric(transactions_items_2024["subtotal"],    errors="coerce").astype("float64")
transactions_items_2024["created_at"] = pd.to_datetime(transactions_items_2024["created_at"], errors="coerce")

transactions_items_2025["transaction_id"] = transactions_items_2025["transaction_id"].astype("string").str.strip()
transactions_items_2025["item_id"] = pd.to_numeric(transactions_items_2025["item_id"], errors="coerce").astype("Int64")
transactions_items_2025["quantity"] = pd.to_numeric(transactions_items_2025["quantity"], errors="coerce").astype("Int64")
transactions_items_2025["unit_price"] = pd.to_numeric(transactions_items_2025["unit_price"], errors="coerce").astype("float64")
transactions_items_2025["subtotal"]    = pd.to_numeric(transactions_items_2025["subtotal"],    errors="coerce").astype("float64")
transactions_items_2025["created_at"] = pd.to_datetime(transactions_items_2025["created_at"], errors="coerce")

# Users Data Importing and Cleaning

users_sample = pd.read_csv('data/users/users_202307.csv', nrows=0)
users_keep_idx = [i for i in range(users_sample.shape[1]) if i not in {1}]

users = pd.concat([pd.read_csv('./data/users/' + f, usecols=users_keep_idx, low_memory=False) for f in users_files], ignore_index=True)

users["user_id"] = pd.to_numeric(users["user_id"], errors="coerce").astype("Int64")
users["birthdate"] = pd.to_datetime(users["birthdate"], errors="coerce").dt.normalize()
users["registered_at"] = pd.to_datetime(users["registered_at"], errors="coerce")

# Menu Items Data Importing and Cleaning

menu_items = pd.read_csv('./data/menu_items/menu_items.csv', nrows=0)
menu_keep_idx = [i for i in range(menu_items.shape[1]) if i not in {4, 5, 6}]
menu_items = pd.read_csv('./data/menu_items/menu_items.csv', usecols=menu_keep_idx, low_memory=False)

menu_items["item_name"] = menu_items["item_name"].astype("string").str.strip()
menu_items["category"] = menu_items["category"].astype("string").str.strip()
menu_items["item_id"] = pd.to_numeric(menu_items["item_id"], errors="coerce").astype("Int64")
menu_items["price"] = pd.to_numeric(menu_items["price"], errors="coerce").astype("float64")

# Stores

stores = pd.read_csv('./data/stores/stores.csv', nrows=0)
stores_keep_idx = [i for i in range(stores.shape[1]) if i not in {2, 3, 6, 7}]
stores = pd.read_csv('./data/stores/stores.csv', usecols=stores_keep_idx, low_memory=False)

stores["store_name"] = stores["store_name"].astype("string").str.strip()
stores["city"] = stores["city"].astype("string").str.strip()
stores["state"] = stores["state"].astype("string").str.strip()
stores["store_id"] = pd.to_numeric(stores["store_id"], errors="coerce").astype("Int64")

# Query 1

q1_transactions_6_to_23_hours = transactions.set_index('created_at').between_time("6:00","23:00")
q1_transactions_6_to_23_hours.reset_index(inplace=True) 
q1_transactions_6_to_23_hours_gt_15 = q1_transactions_6_to_23_hours[q1_transactions_6_to_23_hours["final_amount"] >= 75]

generated_q1_filename = './reports/generated_query1.csv'
q1_transactions_6_to_23_hours_gt_15[["transaction_id", "final_amount"]].sort_values(by="transaction_id", ascending=True).to_csv(generated_q1_filename, index=False, lineterminator='\n')
remove_last_newline(generated_q1_filename)