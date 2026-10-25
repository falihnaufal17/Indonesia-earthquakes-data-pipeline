# Pipeline Data Gempa Bumi

`katalog_gempa.csv` → **Dagster** → ClickHouse **Bronze** → **dbt** Silver → **dbt** Gold → **Superset**
(desain lengkap ada di `data_pipeline_implementation_guide.md`).

## Cara Menjalankan

Ada dua file compose: `docker-compose.db.yml` (database) dan `docker-compose.yml` (Dagster + Superset).
Keduanya memakai jaringan Docker bersama `data_pipeline_net` agar Dagster dan Superset dapat menjangkau ClickHouse
lewat nama host `clickhouse_server`.

```bash
cp .env.example .env                                  # isi nilainya (lihat catatan di bawah)
docker network create data_pipeline_net               # sekali saja
docker compose -f docker-compose.db.yml up -d         # 1. database: ClickHouse :8123/:9000
docker compose up -d --build                          # 2. Dagster :3000, Superset :8088
```

Database harus berjalan lebih dulu sebelum Dagster dijalankan. Nilai `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, dan
`CLICKHOUSE_DB` di `.env` harus sama dengan yang ada di `docker-compose.db.yml`
(bawaan: `myuser` / `mypassword` / `mydb`).

1. Buka Dagster di http://localhost:3000 → **Assets** → *Materialize all* (atau jalankan job `earthquake_pipeline_job`).
   Jadwal harian (pukul 02:00) sudah dibuat, tetapi nonaktif secara default.
2. Setelah run pertama berhasil, buat dashboard sekali saja:
   `python3 superset/bootstrap_dashboards.py` (aman dijalankan ulang) → http://localhost:8088 (`admin` / `admin`,
   dashboard **Analitik Gempa Bumi Indonesia**).

Kredensial disimpan di `.env` (tidak ikut di-commit; salin dari `.env.example`). Ganti nilai bawaan sebelum project dibagikan.

## Data di ClickHouse (`mydb`)

| Layer | Tabel / view | Keterangan |
|---|---|---|
| Bronze | `raw_earthquakes` | CSV apa adanya, semua kolom bertipe `String`; dimuat ulang penuh (truncate + insert) di setiap run |
| Silver | `silver_stg_earthquakes` (view) | sudah bertipe, tervalidasi, dan bebas duplikat berdasarkan (waktu, lintang, bujur) |
| Silver | `silver_fact_earthquake`, `silver_dim_location`, `silver_dim_date` | star schema untuk query SQL ad-hoc |
| Gold | `view_earthquake_analytics_master` | One Big Table untuk Superset |
| Gold | `mart_monthly_trends`, `mart_regional_risk`, `mart_regional_yearly_ranking` | mart hasil agregasi (tren bulanan, risiko wilayah, peringkat wilayah per tahun) |
| Metadata | `meta_data_freshness` | waktu refresh terakhir, rentang data, jumlah baris ditolak/duplikat |
| Metadata | `meta_dq_results` | hasil setiap model/test dbt per run Dagster (hanya ditambah, tidak ditimpa) |

Rekonsiliasi baris: 92.887 baris bronze − 2.735 kejadian duplikat = 90.152 baris fakta.

## Struktur Folder

- `docker-compose.db.yml` – database (ClickHouse dan PostgreSQL); `docker-compose.yml` – Dagster dan Superset
- `ingestion-file/` – file CSV sumber (di-mount read-only ke Dagster)
- `dagster_project/earthquake_pipeline/` – asset, job, dan jadwal Dagster
- `dagster_project/earthquake_dbt/` – project dbt (`models/`, test di `models/schema.yml`, `tests/generic/`)
- `superset/` – image dengan driver ClickHouse, konfigurasi, dan skrip pembuat dashboard
- `dagster/` – image Dagster

## Perbedaan dari Panduan

- **Test:** `dbt_expectations` tidak dipakai (paket tambahan dengan dukungan ClickHouse yang kurang merata); sebagai
  gantinya ada generic test `accepted_range` di `tests/generic/` untuk batas magnitudo, kedalaman, dan koordinat.
- **DDL Bronze:** `insert_df` pada panduan butuh tabel yang sudah ada, jadi asset membuat tabelnya dan melakukan truncate sebelum memuat data.
- **Profil dbt:** disertakan di dalam project dbt dan dikendalikan lewat environment variable, bukan `~/.dbt/profiles.yml`.
- **Peta dasar:** heatmap deck.gl memakai tile OpenStreetMap sehingga tidak perlu token Mapbox.
