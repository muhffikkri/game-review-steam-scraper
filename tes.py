import requests, time, math, random
from datetime import datetime, timezone
import pandas as pd
import os

def get_appid_from_url(url: str) -> int:
    # Contoh URL: https://store.steampowered.com/app/2344520/Diablo_IV/
    # Ambil segmen ke-5 (index 4) setelah split oleh '/'
    parts = [p for p in url.split('/') if p]
    for i, p in enumerate(parts):
        if p == 'app' and i + 1 < len(parts):
            return int(parts[i+1])
    raise ValueError("URL tidak valid untuk mendeteksi APPID.")

def fetch_reviews(app_id: int,
                  start_dt: datetime,
                  end_dt: datetime,
                  language: str = "all",
                  include_offtopic: bool = False,
                  per_page: int = 100,
                  sleep_sec: float = 0.3):
    """
    Generator yang mengembalikan dict review untuk app_id dalam rentang [start_dt, end_dt).
    """
    url = f"https://store.steampowered.com/appreviews/{app_id}"
    params = {
        "json": 1,
        "language": language,             # "all" untuk semua bahasa
        "review_type": "all",
        "purchase_type": "all",
        "filter": "recent",               # penting agar pagination berakhir (bisa empty list)
        "num_per_page": per_page,
        "cursor": "*" ,
        "filter_offtopic_activity": 0 if not include_offtopic else 1
    }

    start_ts = int(start_dt.replace(tzinfo=timezone.utc).timestamp())
    end_ts   = int(end_dt.replace(tzinfo=timezone.utc).timestamp())

    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        reviews = data.get("reviews", [])
        if not reviews:
            break  # habis

        for rv in reviews:
            ts = int(rv.get("timestamp_created", 0))
            if start_ts <= ts < end_ts:
                yield {
                    "app_id": app_id,
                    "recommendationid": rv.get("recommendationid"),
                    "author_steamid": rv.get("author", {}).get("steamid"),
                    "language": rv.get("language"),
                    "review_text": rv.get("review"),
                    "timestamp_created": ts,
                    "datetime_created_utc": datetime.utcfromtimestamp(ts),
                    "voted_up": bool(rv.get("voted_up")),
                    "votes_up": rv.get("votes_up"),
                    "votes_funny": rv.get("votes_funny"),
                    "comment_count": rv.get("comment_count"),
                    "steam_purchase": rv.get("steam_purchase"),
                    "received_for_free": rv.get("received_for_free"),
                    "playtime_at_review": rv.get("author", {}).get("playtime_at_review"),
                }

        # Optimisasi: kalau batch ini sudah melewati awal rentang (lebih tua dari start_dt),
        # kita bisa berhenti. Karena 'recent' mengurut dari terbaru ke lebih lama.
        oldest_ts_in_batch = int(reviews[-1].get("timestamp_created", 0))
        if oldest_ts_in_batch < start_ts:
            break

        # lanjutkan paging
        params["cursor"] = data.get("cursor", params["cursor"])
        time.sleep(sleep_sec)

def monthly_sample(df: pd.DataFrame, n_per_month: int = 10, seed: int = 42, by_top_helpful: bool = False):
    df = df.copy()
    df["month"] = df["datetime_created_utc"].dt.to_period("M").astype(str)

    if by_top_helpful:
        # ambil 10 teratas per bulan berdasarkan votes_up (helpfulness)
        sampled = (df.sort_values(["month", "votes_up"], ascending=[True, False])
                     .groupby("month", group_keys=False)
                     .head(n_per_month))
    else:
        # random sample per bulan - untuk mengatasi warning, gunakan cara alternatif
        result = []
        rng = random.Random(seed)
        for month, group in df.groupby("month"):
            if len(group) > 0:
                n_sample = min(n_per_month, len(group))
                # Menggunakan sample dengan random_state
                sampled_group = group.sample(n=n_sample, 
                                           random_state=rng.randint(0, 10**9))
                result.append(sampled_group)
        
        # Gabungkan semua hasil sampling
        sampled = pd.concat(result) if result else pd.DataFrame(columns=df.columns)
    
    return sampled.reset_index(drop=True)

def monthly_summary(df: pd.DataFrame):
    df = df.copy()
    df["month"] = df["datetime_created_utc"].dt.to_period("M").astype(str)
    
    # Menggunakan cara alternatif tanpa include_groups
    result = {}
    for month, group in df.groupby("month"):
        result[month] = {
            'total_reviews': len(group),
            'positive': sum(group['voted_up'])
        }
    
    # Konversi ke DataFrame
    agg = pd.DataFrame.from_dict(result, orient='index')
    agg.index.name = 'month'
    agg = agg.reset_index()
    
    # Tambahkan kolom tambahan
    agg["negative"] = agg["total_reviews"] - agg["positive"]
    agg["pos_share"] = (agg["positive"] / agg["total_reviews"]).round(3)
    
    return agg

if __name__ == "__main__":
    # Periksa dependensi yang diperlukan
    try:
        import openpyxl
    except ImportError:
        print("Package 'openpyxl' tidak ditemukan.")
        print("Silakan install dengan perintah: pip install openpyxl")
        print("Lalu jalankan script ini kembali.")
        exit(1)
        
    # ---- PARAMETER UTAMA ----
    APP_ID = 367520  # Diablo IV; ganti dengan 3410180 untuk Overlooting, atau gunakan get_appid_from_url
    # APP_ID = get_appid_from_url("https://store.steampowered.com/app/2344520/Diablo_IV/")

    START = datetime(2017, 1, 1)
    # sekarang (UTC) – boleh diganti ke tanggal tertentu jika perlu
    END   = datetime(2025, 9, 3)

    # ---- AMBIL DATA ----
    rows = list(fetch_reviews(APP_ID, START, END, language="english",
                              include_offtopic=False, per_page=100, sleep_sec=0.25))
    df = pd.DataFrame(rows)
    if df.empty:
        print("Tidak ada review dalam rentang waktu yang diminta.")
    else:
        # Buat folder output jika belum ada
        output_folder = "steam_reviews_excel"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            
        # Nama game untuk penamaan file
        game_name = "Hollow Knight" if APP_ID == 238370 else f"Game_{APP_ID}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # ---- SAMPLING 10/BULAN ----
        sample10 = monthly_sample(df, n_per_month=10, seed=123, by_top_helpful=False)
        
        # Simpan sebagai Excel dengan format yang lebih rapi
        excel_file = os.path.join(output_folder, f"{game_name}_sample_reviews_{timestamp}.xlsx")
        
        # Membuat Excel writer dengan engine openpyxl
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Menyimpan sampel 10 per bulan
            sample10.to_excel(writer, sheet_name='Reviews Sample', index=False)
            
            # Format kolom datetime agar lebih mudah dibaca
            workbook = writer.book
            worksheet = writer.sheets['Reviews Sample']
            
            # Menyesuaikan lebar kolom berdasarkan isi
            for idx, col in enumerate(sample10.columns):
                max_length = max(
                    sample10[col].astype(str).apply(len).max(),
                    len(str(col))
                ) + 2
                # Batasi lebar maksimum untuk kolom review_text
                if col == 'review_text':
                    max_length = min(max_length, 100)  # Batasi maks 100 karakter
                # Setel lebar kolom (kolom Excel dimulai dari 1)
                col_letter = chr(65 + idx) if idx < 26 else chr(64 + idx // 26) + chr(65 + idx % 26)
                worksheet.column_dimensions[col_letter].width = max_length
            
            # Menyimpan ringkasan bulanan
            summary = monthly_summary(df)
            summary.to_excel(writer, sheet_name='Monthly Summary', index=False)
            
            # Format worksheet ringkasan bulanan
            worksheet = writer.sheets['Monthly Summary']
            for idx, col in enumerate(summary.columns):
                max_length = max(
                    summary[col].astype(str).apply(len).max(),
                    len(str(col))
                ) + 2
                col_letter = chr(65 + idx) if idx < 26 else chr(64 + idx // 26) + chr(65 + idx % 26)
                worksheet.column_dimensions[col_letter].width = max_length
        
        print(f"Tersimpan: {excel_file}")
        
        # Tetap simpan CSV untuk kompatibilitas jika diperlukan, tapi di dalam folder output
        # untuk menghindari permission error
        csv_file = os.path.join(output_folder, f"{game_name}_sample_reviews_{timestamp}.csv")
        sample10.to_csv(csv_file, index=False, encoding="utf-8")
        print(f"Juga tersimpan sebagai CSV: {csv_file}")
        
        summary_csv = os.path.join(output_folder, f"{game_name}_monthly_summary_{timestamp}.csv")
        summary.to_csv(summary_csv, index=False, encoding="utf-8")
        print(f"Juga tersimpan sebagai CSV: {summary_csv}")

        # ---- OPSIONAL: RINGKASAN GLOBAL DARI QUERY_SUMMARY ----
        # Untuk mendapatkan review_score_desc global, cukup hit endpoint sekali:
        url_once = f"https://store.steampowered.com/appreviews/{APP_ID}?json=1"
        params_once = {"language": "english", "review_type": "all", "purchase_type": "all",
                       "filter": "recent", "num_per_page": 1, "cursor": "*"}
        try:
            q = requests.get(url_once, params=params_once, timeout=15).json()
            qs = q.get("query_summary", {})
            print("Overall review (global):", qs.get("review_score_desc"),
                  "| total +:", qs.get("total_positive"),
                  "| total -:", qs.get("total_negative"),
                  "| total:", qs.get("total_reviews"))
        except Exception as e:
            print("Gagal ambil query_summary:", e)
