"""Create the ClickHouse connection, datasets, charts and dashboard in Superset.

Idempotent: re-running updates existing objects (matched by name) instead of duplicating them.
Run from the host after `docker compose up -d` and a successful Dagster materialization:

    python3 superset/bootstrap_dashboards.py
"""
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN = os.getenv("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

CH_DB = os.getenv("CLICKHOUSE_DB", "mydb")
CH_URI = "clickhousedb://{u}:{p}@{h}:{port}/{db}".format(
    u=os.getenv("CLICKHOUSE_USER", "myuser"),
    p=os.getenv("CLICKHOUSE_PASSWORD", "mypassword"),
    h=os.getenv("CLICKHOUSE_HOST", "clickhouse_server"),  # container-to-container hostname
    port=os.getenv("CLICKHOUSE_PORT", "8123"),
    db=CH_DB,
)
DB_NAME = "ClickHouse - Earthquakes"
DASHBOARD_TITLE = "Analitik Gempa Bumi Indonesia"
OSM_TILES = "tile://https://tile.openstreetmap.org/{z}/{x}/{y}.png"  # no Mapbox token needed

_token = None


def api(method, path, body=None):
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {_token}"} if _token else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as err:
        sys.exit(f"{method} {path} -> {err.code}: {err.read().decode()[:800]}")


def find_id(resource, column, value):
    q = f"(filters:!((col:{column},opr:eq,value:'{urllib.parse.quote(value)}')))"
    result = api("GET", f"/api/v1/{resource}/?q={q}")
    return result["result"][0]["id"] if result.get("count") else None


def upsert(resource, column, value, payload):
    existing = find_id(resource, column, value)
    if existing:
        api("PUT", f"/api/v1/{resource}/{existing}", payload)
        return existing
    return api("POST", f"/api/v1/{resource}/", payload)["id"]


def sql_metric(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


# --------------------------------------------------------------------------------------
# Connection + datasets
# --------------------------------------------------------------------------------------
resp = api("POST", "/api/v1/security/login",
           {"username": ADMIN, "password": PASSWORD, "provider": "db", "refresh": True})
_token = resp["access_token"]

db_id = upsert("database", "database_name", DB_NAME, {
    "database_name": DB_NAME,
    "sqlalchemy_uri": CH_URI,
    "expose_in_sqllab": True,
})
print(f"database id={db_id}")

DQ_SQL = (
    "SELECT executed_at, node_name, "
    "multiIf(resource_type = 'test', 'Tes', resource_type = 'model', 'Model', "
    "resource_type = 'source', 'Sumber', resource_type) AS resource_type, "
    "multiIf(status = 'pass', 'Lulus', status = 'success', 'Berhasil', status = 'fail', 'Gagal', "
    "status = 'error', 'Error', status = 'skipped', 'Dilewati', status = 'warn', 'Peringatan', status) AS status, "
    "message "
    f"FROM {CH_DB}.meta_dq_results "
    f"WHERE dagster_run_id = (SELECT argMax(dagster_run_id, executed_at) FROM {CH_DB}.meta_dq_results)"
)

# Column headers shown in tables, tooltips and filters.
VERBOSE_NAMES = {
    "earthquake_id": "ID Gempa", "event_time": "Waktu Kejadian", "event_date": "Tanggal Kejadian",
    "year": "Tahun", "quarter": "Kuartal", "month": "Bulan", "month_name": "Nama Bulan",
    "month_start": "Bulan", "year_month": "Tahun-Bulan", "day_of_week_name": "Hari",
    "is_weekend": "Akhir Pekan", "region_name": "Wilayah", "latitude": "Lintang",
    "longitude": "Bujur", "depth_km": "Kedalaman (km)", "magnitude": "Magnitudo",
    "magnitude_class": "Kelas Magnitudo", "depth_class": "Kelas Kedalaman",
    "ingested_at": "Waktu Ingest", "total_earthquakes": "Jumlah Gempa",
    "avg_magnitude": "Magnitudo Rata-rata", "max_magnitude": "Magnitudo Maks",
    "avg_depth_km": "Kedalaman Rata-rata (km)", "significant_earthquakes": "Gempa Signifikan (M≥5)",
    "rank_in_year": "Peringkat", "pct_shallow": "% Dangkal", "centroid_latitude": "Lintang Pusat", "centroid_longitude": "Bujur Pusat",
    "refreshed_at": "Diperbarui Pada", "last_ingested_at": "Terakhir Diingest",
    "earliest_event_time": "Kejadian Terawal", "latest_event_time": "Kejadian Terbaru",
    "bronze_rows": "Baris Data Mentah", "fact_rows": "Baris Tabel Fakta",
    "rows_rejected_or_deduplicated": "Baris Ditolak/Duplikat", "executed_at": "Waktu Eksekusi",
    "node_name": "Nama Node", "resource_type": "Tipe", "status": "Status", "message": "Pesan",
}


def apply_verbose_names(dataset_id):
    columns = api("GET", f"/api/v1/dataset/{dataset_id}")["result"]["columns"]
    # PUT replaces the column list, so send every column back with its id.
    payload = [
        {"id": c["id"], "column_name": c["column_name"],
         "verbose_name": VERBOSE_NAMES.get(c["column_name"], c.get("verbose_name"))}
        for c in columns
    ]
    api("PUT", f"/api/v1/dataset/{dataset_id}", {"columns": payload})


datasets = {}
for table, sql in [
    ("view_earthquake_analytics_master", None),
    ("mart_monthly_trends", None),
    ("mart_regional_risk", None),
    ("mart_regional_yearly_ranking", None),
    ("meta_data_freshness", None),
    ("dq_latest_run", DQ_SQL),
]:
    existing = find_id("dataset", "table_name", table)
    if existing:
        datasets[table] = existing
        if sql:
            api("PUT", f"/api/v1/dataset/{existing}", {"sql": sql})
    else:
        payload = {"database": db_id, "schema": CH_DB, "table_name": table}
        if sql:
            payload["sql"] = sql
        datasets[table] = api("POST", "/api/v1/dataset/", payload)["id"]
    api("PUT", f"/api/v1/dataset/{datasets[table]}/refresh")  # pick up current columns
    apply_verbose_names(datasets[table])
    print(f"dataset {table} id={datasets[table]}")

MASTER, MONTHLY, REGIONAL, YEARLY_RANK, FRESH, DQ = (
    datasets[k] for k in
    ("view_earthquake_analytics_master", "mart_monthly_trends", "mart_regional_risk",
     "mart_regional_yearly_ranking", "meta_data_freshness", "dq_latest_run")
)

# --------------------------------------------------------------------------------------
# Charts: (name, dataset id, viz_type, params, dashboard width, height)
# --------------------------------------------------------------------------------------
VIEWPORT = {"longitude": 118, "latitude": -2.5, "zoom": 3.6, "bearing": 0, "pitch": 0}

charts = [
    # Executive KPI big numbers
    ("KPI - Total Gempa", MASTER, "big_number_total", {
        "metric": sql_metric("COUNT(*)", "Total gempa"),
        "y_axis_format": "SMART_NUMBER", "header_font_size": 0.4, "subheader_font_size": 0.15,
    }, 3, 22),
    ("KPI - Magnitudo Terkuat", MASTER, "big_number_total", {
        "metric": sql_metric("MAX(magnitude)", "Magnitudo terkuat"),
        "y_axis_format": ",.1f", "header_font_size": 0.4, "subheader_font_size": 0.15,
    }, 3, 22),
    ("KPI - Rata-rata Kedalaman (km)", MASTER, "big_number_total", {
        "metric": sql_metric("AVG(depth_km)", "Rata-rata kedalaman (km)"),
        "y_axis_format": ",.1f", "header_font_size": 0.4, "subheader_font_size": 0.15,
    }, 3, 22),
    ("KPI - Pemeriksaan Kualitas Data Gagal", DQ, "big_number_total", {
        "metric": sql_metric("countIf(status IN ('Gagal', 'Error'))", "Pemeriksaan gagal (run terakhir)"),
        "y_axis_format": "SMART_NUMBER", "header_font_size": 0.4, "subheader_font_size": 0.15,
    }, 3, 22),
    # deck.gl geospatial heatmap
    ("Peta Panas Gempa (deck.gl)", MASTER, "deck_heatmap", {
        "spatial": {"type": "latlong", "latCol": "latitude", "lonCol": "longitude"},
        "size": sql_metric("MAX(magnitude)", "Magnitudo"),
        "row_limit": 100000, "mapbox_style": OSM_TILES, "viewport": VIEWPORT,
        "autozoom": False, "intensity": 2, "radius_pixels": 25, "aggregation": "SUM",
        "linear_color_scheme": "superset_seq_1", "js_columns": [],
    }, 7, 70),
    # Depth vs magnitude (event level, capped sample)
    ("Kedalaman vs Magnitudo", MASTER, "bubble_v2", {
        "entity": "earthquake_id",
        "x": sql_metric("MAX(depth_km)", "Kedalaman (km)"),
        "y": sql_metric("MAX(magnitude)", "Magnitudo"),
        "size": sql_metric("1", "Ukuran"),
        "series": "depth_class",
        "max_bubble_size": "4", "row_limit": 5000, "limit": 5000,
        "x_axis_title": "Kedalaman (km)", "y_axis_title": "Magnitudo",
        "show_legend": True, "opacity": 0.5,
    }, 5, 70),
    # Time-series
    ("Jumlah Gempa per Bulan", MONTHLY, "echarts_timeseries_line", {
        "x_axis": "month_start", "time_grain_sqla": "P1M",
        "metrics": [sql_metric("SUM(total_earthquakes)", "Jumlah gempa")],
        "groupby": [], "row_limit": 10000, "show_legend": False, "rich_tooltip": True,
        "y_axis_format": "SMART_NUMBER",
        "x_axis_title": "Bulan", "x_axis_title_margin": 30,
        "y_axis_title": "Jumlah gempa", "y_axis_title_margin": 50, "y_axis_title_position": "Top",
    }, 6, 50),
    ("Magnitudo Bulanan (rata-rata / maks)", MONTHLY, "echarts_timeseries_line", {
        "x_axis": "month_start", "time_grain_sqla": "P1M",
        "metrics": [sql_metric("AVG(avg_magnitude)", "Magnitudo rata-rata"),
                    sql_metric("MAX(max_magnitude)", "Magnitudo maks")],
        "groupby": [], "row_limit": 10000, "show_legend": True, "rich_tooltip": True,
        "y_axis_format": ",.1f",
        "x_axis_title": "Bulan", "x_axis_title_margin": 30,
        "y_axis_title": "Magnitudo", "y_axis_title_margin": 50, "y_axis_title_position": "Top",
    }, 6, 50),
    # Regional risk
    ("15 Wilayah dengan Gempa Terbanyak", REGIONAL, "echarts_timeseries_bar", {
        "x_axis": "region_name", "x_axis_sort": "Jumlah gempa", "x_axis_sort_asc": False,
        "metrics": [sql_metric("SUM(total_earthquakes)", "Jumlah gempa")],
        "groupby": [], "row_limit": 15, "orientation": "vertical", "show_legend": False,
        "y_axis_format": "SMART_NUMBER", "xAxisLabelRotation": 45,
        "orderby": [[sql_metric("SUM(total_earthquakes)", "Jumlah gempa"), False]],
    }, 6, 60),
    ("Tabel Risiko Wilayah", REGIONAL, "table", {
        "query_mode": "raw",
        "all_columns": ["region_name", "total_earthquakes", "significant_earthquakes",
                        "max_magnitude", "avg_depth_km", "pct_shallow"],
        "order_by_cols": ['["total_earthquakes", false]'],
        "row_limit": 51, "include_search": True, "page_length": 10,
    }, 6, 60),
    # Ranking: top regions per year (rank 1 = most earthquakes). Responds to the Tahun filter.
    ("Peringkat Wilayah dengan Gempa Terbanyak per Tahun", YEARLY_RANK, "table", {
        "query_mode": "raw",
        "all_columns": ["year", "rank_in_year", "region_name", "total_earthquakes",
                        "max_magnitude", "avg_magnitude", "significant_earthquakes"],
        "adhoc_filters": [{"expressionType": "SIMPLE", "clause": "WHERE", "subject": "rank_in_year",
                           "operator": "<=", "comparator": 10}],
        "order_by_cols": ['["year", false]', '["rank_in_year", true]'],
        "row_limit": 500, "include_search": True, "page_length": 10, "server_pagination": False,
        "table_timestamp_format": "%Y", "show_cell_bars": False,
    }, 12, 70),
    # Data quality & freshness metadata
    ("Kesegaran Data", FRESH, "table", {
        "query_mode": "raw",
        "all_columns": ["refreshed_at", "last_ingested_at", "earliest_event_time",
                        "latest_event_time", "bronze_rows", "fact_rows",
                        "rows_rejected_or_deduplicated"],
        "row_limit": 1, "include_search": False,
    }, 12, 22),
    ("Hasil Kualitas Data (run terakhir)", DQ, "table", {
        "query_mode": "aggregate", "groupby": ["resource_type", "status"],
        "metrics": [sql_metric("COUNT(*)", "Jumlah node")],
        "row_limit": 100, "include_search": False,
    }, 12, 30),
]

dash_id = upsert("dashboard", "dashboard_title", DASHBOARD_TITLE,
                 {"dashboard_title": DASHBOARD_TITLE, "published": True})
print(f"dashboard id={dash_id}")

placed = []  # (chart_id, name, width, height)
for name, ds_id, viz, params, width, height in charts:
    form_data = {"datasource": f"{ds_id}__table", "viz_type": viz, "adhoc_filters": [], **params}  # params may override adhoc_filters
    chart_id = upsert("chart", "slice_name", name, {
        "slice_name": name, "viz_type": viz,
        "datasource_id": ds_id, "datasource_type": "table",
        "params": json.dumps(form_data), "dashboards": [dash_id],
    })
    placed.append((chart_id, name, width, height))
    print(f"chart {name} id={chart_id}")

# Layout: pack charts into rows of 12 grid columns.
position = {
    "DASHBOARD_VERSION_KEY": "v2",
    "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
    "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
}
row, used, row_no = None, 12, 0
for chart_id, name, width, height in placed:
    if used + width > 12:
        row_no += 1
        row = f"ROW-{row_no}"
        position[row] = {"type": "ROW", "id": row, "children": [],
                         "parents": ["ROOT_ID", "GRID_ID"],
                         "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        position["GRID_ID"]["children"].append(row)
        used = 0
    key = f"CHART-{chart_id}"
    position[key] = {"type": "CHART", "id": key, "children": [],
                     "parents": ["ROOT_ID", "GRID_ID", row],
                     "meta": {"chartId": chart_id, "sliceName": name,
                              "width": width, "height": height}}
    position[row]["children"].append(key)
    used += width

# Native filter: Year (multi-select). Only datasets with a `year` column can be filtered,
# so charts on the regional / freshness / data-quality datasets are excluded from its scope.
NO_YEAR_DATASETS = {REGIONAL, FRESH, DQ}
excluded_charts = [cid for cid, (_, ds_id, *_rest) in zip((p[0] for p in placed), charts)
                   if ds_id in NO_YEAR_DATASETS]
year_filter = {
    "id": "NATIVE_FILTER-year",
    "name": "Tahun",
    "type": "NATIVE_FILTER",
    "filterType": "filter_select",
    "targets": [{"datasetId": MASTER, "column": {"name": "year"}}],
    "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded_charts},
    "cascadeParentIds": [],
    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
    "controlValues": {"multiSelect": True, "enableEmptyFilter": False,
                      "defaultToFirstItem": False, "inverseSelection": False,
                      "searchAllOptions": False, "sortAscending": True},
    "description": "Filter dashboard berdasarkan tahun kejadian",
}

api("PUT", f"/api/v1/dashboard/{dash_id}", {
    "position_json": json.dumps(position),
    "json_metadata": json.dumps({"native_filter_configuration": [year_filter]}),
    "published": True,
})
print(f"\nDashboard ready: {BASE}/superset/dashboard/{dash_id}/")
