import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from supabase import create_client, Client

load_dotenv()
SUPABASE_URI = os.getenv("SUPABASE_URI")
SUPABASE_URL = os.getenv("SUPABASE_URL")    # <- ENV BARU
SUPABASE_KEY = os.getenv("SUPABASE_KEY")    # <- ENV BARU

if not SUPABASE_URI or not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Pastikan SUPABASE_URI, SUPABASE_URL, dan SUPABASE_KEY ada di .env")

engine = create_engine(SUPABASE_URI, connect_args={"sslmode": "require"})

def clean_transactions(df, file_name):
    """Pembersihan kualitas data, missing values, deduplikasi, dan mencatat ringkasan (summary)"""
    
    # Inisialisasi kamus ringkasan metrik
    summary = {
        "file_name": file_name,
        "initial_rows": len(df),
        "duplicates_removed": 0,
        "null_purpose_filled": 0,
        "null_bank_filled": 0,
        "null_partner_filled": 0,
        "final_rows": 0
    }
    
    # 1. Bersihkan nama kolom
    df.columns = df.columns.str.replace('"', '').str.strip().str.lower()
    
    # 2. Tangani duplikat baris
    before_dup = len(df)
    df = df.drop_duplicates()
    summary["duplicates_removed"] = before_dup - len(df)
        
    # 3. Validasi & Konversi Format Tanggal
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
    # 4. Penanganan Missing Values & Pencatatan Statistik Imputasi
    if 'purpose' in df.columns:
        df['purpose'] = df['purpose'].astype(str).str.strip()
        df['purpose'] = df['purpose'].replace(r'^\s*$', pd.NA, regex=True)
        summary["null_purpose_filled"] = int(df['purpose'].isnull().sum())
        df['purpose'] = df['purpose'].fillna('Unknown')
        
    if 'bank' in df.columns:
        summary["null_bank_filled"] = int(df['bank'].isnull().sum())
        df['bank'] = df['bank'].fillna('Internal/Unknown')
        
    if 'account_partern_id' in df.columns:
        summary["null_partner_filled"] = int(df['account_partern_id'].isnull().sum())
        df['account_partern_id'] = df['account_partern_id'].fillna(0)
        
    # Validasi tipe data numerik
    for col in ['amount', 'balance', 'trans_id', 'account_id']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    summary["final_rows"] = len(df)
    return df, summary

def print_execution_summary(summary):
    """Mencetak laporan ringkasan yang rapi di CLI/Terminal"""
    print("\n" + "="*40)
    print(f" 📊 SUMMARY LAPORAN ETL: {summary['file_name']}")
    print("="*40)
    print(f" • Total Baris Awal       : {summary['initial_rows']:,} baris")
    print(f" • Baris Duplikat Dihapus : {summary['duplicates_removed']:,} baris")
    print(f" • Imputasi Kolom 'purpose': {summary['null_purpose_filled']:,} data kosong diisi")
    print(f" • Imputasi Kolom 'bank'   : {summary['null_bank_filled']:,} data kosong diisi")
    print(f" • Imputasi Partner ID    : {summary['null_partner_filled']:,} data kosong diisi")
    print(f" • Total Baris Final/Load : {summary['final_rows']:,} baris")
    print("="*40 + "\n")

def export_master_parquet_to_supabase():
    """Menarik seluruh data dari tabel dan mengunggahnya sebagai Parquet ke Supabase Storage"""
    print("\n=== MEMULAI EKSPOR PARQUET UNTUK POWER BI ===")
    try:
        # 1. Tarik seluruh data dari database
        print("Menarik data terbaru dari tabel 'trnx'...")
        full_df = pd.read_sql("SELECT * FROM trnx", engine)
        
        # 2. Simpan sebagai file Parquet lokal sementara
        parquet_filename = "master_transactions.parquet"
        full_df.to_parquet(parquet_filename, index=False)
        print(f"File {parquet_filename} berhasil dibuat lokal.")
        
        # 3. Upload ke Supabase Storage
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        bucket_name = "powerbi-export"
        
        with open(parquet_filename, "rb") as f:
            print(f"Mengunggah ke Supabase bucket: '{bucket_name}'...")
            # upsert=True akan menimpa (overwrite) file yang lama
            supabase.storage.from_(bucket_name).upload(
                file=f,
                path=parquet_filename,
                file_options={"upsert": "true", "content-type": "application/octet-stream"}
            )
        
        print("-> Sukses! File master_transactions.parquet sudah diperbarui.")
        
        # Hapus file Parquet lokal agar bersih
        os.remove(parquet_filename)
        
    except Exception as e:
        print(f"[ERROR] Gagal mengekspor Parquet: {e}")

def run_transaction_etl():
    incoming_dir = "data/transactions/incoming"
    archive_dir = "data/transactions/archive"
    
    os.makedirs(incoming_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)
    
    files = [f for f in os.listdir(incoming_dir) if f.endswith('.csv')]
    
    if not files:
        print("Tidak ada file transaksi baru di folder incoming.")
        return

    print(f"=== MEMULAI ETL TRANSAKSI ({len(files)} file ditemukan) ===\n")

    for file_name in files:
        file_path = os.path.join(incoming_dir, file_name)
        print(f"Memproses file: {file_name}...")
        
        try:
            df = pd.read_csv(file_path, low_memory=False)
            
            df_cleaned, summary = clean_transactions(df, file_name)
            
            # Load ke Supabase
            df_cleaned.to_sql('trnx', engine, if_exists='append', index=False)
            
            # Tampilkan ringkasan CLI
            print_execution_summary(summary)
            print(f"-> Sukses mengunggah {len(df_cleaned)} baris ke tabel 'trnx'.")
            
            # Arsifkan file
            archive_path = os.path.join(archive_dir, file_name)
            os.rename(file_path, archive_path)
            print(f"-> File dipindahkan ke arsip: {archive_path}\n")
            
        except Exception as e:
            print(f"[ERROR] Gagal memproses file {file_name}: {e}\n")

    # Setelah semua file diproses, ekspor ke Parquet untuk Power BI
    if files:
        export_master_parquet_to_supabase()
        
    print("=== PROSES ETL TRANSAKSI SELESAI ===")

if __name__ == "__main__":
    run_transaction_etl()