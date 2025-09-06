import duckdb

RUTA_DATA_0RIGINAL = "talkingdata-adtracking-fraud-detection/train.csv"
RUTA_DATA_TRANSFORMADA = "talkingdata-adtracking-fraud-detection/train.parquet"

# duckdb.sql(f"SELECT * FROM '{RUTA_DATA_0RIGINAL}' LIMIT 5").show()

# duckdb.sql(f"SELECT COUNT (*) FROM '{RUTA_DATA_0RIGINAL}'").show()

configuracion = {
    "threads": 4,
    "memory_limit": "4GB",
    "temp_directory": "/tmp"
}

conexion = duckdb.connect(config=configuracion)
conexion.sql("PRAGMA enable_progress_bar = true;")

conexion.sql(f"""
COPY (
    SELECT ip, app, device, os, channel,
        CAST(click_time AS TIMESTAMP) AS click_time,
        is_attributed
    FROM read_csv_auto('{RUTA_DATA_0RIGINAL}', SAMPLE_SIZE=-1)
) TO '{RUTA_DATA_TRANSFORMADA}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")