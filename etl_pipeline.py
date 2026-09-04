import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
SUPABASE_URI = os.getenv("SUPABASE_URI")

if not SUPABASE_URI:
    raise ValueError("SUPABASE_URI tidak ditemukan! Pastikan sudah diatur di file .env")

engine = create_engine(SUPABASE_URI)

def clean_data(df, table_name):
    """Pembersihan komprehensif: deduplikasi, missing values, dan format tipe data"""
    
    # 1. Hapus spasi dan tanda kutip pada nama kolom, ubah jadi huruf kecil
    df.columns = df.columns.str.replace('"', '').str.strip().str.lower()
    
    # 2. Tangani duplikat global
    initial_rows = len(df)
    df = df.drop_duplicates()
    if initial_rows > len(df):
        print(f"-> Info: Menghapus {initial_rows - len(df)} baris duplikat pada {table_name}.")

    # 3. Pembersihan spesifik per tabel
    if table_name == "account":
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%y%m%d', errors='coerce')
            
    elif table_name == "card":
        if 'issued' in df.columns:
            # Ambil bagian tanggal saja sebelum spasi (misal: '931107 00:00:00' -> '931107')
            issued_str = df['issued'].astype(str).str.split().str[0]
            df['issued'] = pd.to_datetime(issued_str, format='%y%m%d', errors='coerce')
        if 'type' in df.columns:
            df['type'] = df['type'].str.replace('"', '').str.strip()
            
    elif table_name == "client":
        if 'birth_number' in df.columns:
            df['birth_number'] = df['birth_number'].astype(str).str.replace('"', '').str.strip()
            
    elif table_name == "loan":
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%y%m%d', errors='coerce')
        if 'status' in df.columns:
            df['status'] = df['status'].str.replace('"', '').str.strip()
            
    elif table_name == "district":
        rename_mapping = {
            'a1': 'district_id',
            'a2': 'district_name',
            'a3': 'region',
            'a4': 'population',
            'a5': 'municipality_lt_499',
            'a6': 'municipality_500_1999',
            'a7': 'municipality_2000_9999',
            'a8': 'municipality_gt_10000',
            'a9': 'number_of_cities',
            'a10': 'ratio_urban_inhabitants',
            'a11': 'average_salary',
            'a12': 'unemployment_rate_prev',
            'a13': 'unemployment_rate_curr',
            'a14': 'entrepreneurs_per_1000',
            'a15': 'crimes_prev',
            'a16': 'crimes_curr'
        }
        df = df.rename(columns=rename_mapping)

        # Imputasi missing values pada A12 dan A15 menggunakan median
        for col in ['a12', 'a15']:
            if col in df.columns and df[col].isnull().sum() > 0:
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                print(f"-> Info: Missing value pada {table_name}.{col} diisi dengan median ({median_val}).")
                
    elif table_name == "trnx":
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Tangani missing values masif pada trnx_16
        if 'purpose' in df.columns:
            df['purpose'] = df['purpose'].fillna('Unknown')
        if 'bank' in df.columns:
            df['bank'] = df['bank'].fillna('Internal/Unknown')
        if 'account_partern_id' in df.columns:
            df['account_partern_id'] = df['account_partern_id'].fillna(0)
            
    return df

def run_etl():
    print("=== MEMULAI PROSES ADVANCED ETL & DATA CLEANING ===\n")
    
    # Konfigurasi pembacaan berdasarkan struktur asli file
    datasets = {
        "account": {"path": "data/account.csv", "sep": ",", "quote": '"'},
        "card": {"path": "data/card.csv", "sep": ";", "quote": '"'},
        "client": {"path": "data/client.csv", "sep": ";", "quote": '"'},
        "disp": {"path": "data/disp.csv", "sep": ",", "quote": '"'},
        "district": {"path": "data/district.csv", "sep": ",", "quote": '"'},
        "loan": {"path": "data/loan.csv", "sep": ";", "quote": '"'},
        "order": {"path": "data/order.csv", "sep": ",", "quote": '"'},
        "trnx": {"path": "data/trnx_16.csv", "sep": ",", "quote": '"'}
    }

    for table_name, config in datasets.items():
        file_path = config["path"]
        
        if not os.path.exists(file_path):
            print(f"[WARNING] File {file_path} tidak ditemukan, melewati tabel '{table_name}'...")
            continue
            
        print(f"Memproses tabel '{table_name}'...")
        try:
            # Membaca CSV dengan separator dan quotechar yang tepat
            df = pd.read_csv(
                file_path, 
                sep=config["sep"], 
                quotechar=config["quote"], 
                low_memory=False
            )
            
            # Jalankan pembersihan data tingkat lanjut
            df_cleaned = clean_data(df, table_name)
            
            # Load ke Supabase PostgreSQL
            df_cleaned.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"-> Sukses! Tabel '{table_name}' bersih dan masuk ke Supabase ({len(df_cleaned)} baris).\n")
            
        except Exception as e:
            print(f"[ERROR] Gagal memproses tabel '{table_name}': {e}\n")

    print("=== SELURUH PROSES ADVANCED ETL SELESAI ===")

if __name__ == "__main__":
    run_etl()