import pandas as pd

datos = pd.read_csv("data/train_sample.csv", parse_dates=["click_time"])

# Extrae valores de fecha
datos["anio"] = datos["click_time"].dt.year
datos["mes"] = datos["click_time"].dt.month
datos["dia_del_mes"] = datos["click_time"].dt.day
datos["hora"] = datos["click_time"].dt.hour
datos["minuto"] = datos["click_time"].dt.minute
datos["segundo"] = datos["click_time"].dt.second

# Mapeo dia de la semana
datos["dia_semana"] = datos["click_time"].dt.dayofweek
mapa_dias_semana = {
    0: "lunes",
    1: "martes",
    2: "miércoles",
    3: "jueves",
    4: "viernes",
    5: "sábado",
    6: "domingo"
}
datos["nombre_dia_semana"] = datos["dia_semana"].map(mapa_dias_semana)

# Flags
mascara_fin_de_semana = datos["dia_semana"].isin([5, 6])
datos["es_fin_de_semana"] = mascara_fin_de_semana

mascara_noche = (datos["hora"] >= 0) & (datos["hora"] < 6)
datos["es_noche"] = mascara_noche

# Franja horaria
limites_franja = [0, 6, 12, 18, 24]
etiquetas_franja = ["madrugada", "mañana", "tarde", "noche"]
datos["franja_horaria"] = pd.cut(
    datos["hora"],
    bins=limites_franja,
    right=False,
    labels=etiquetas_franja,
    include_lowest=True
)

# Segundos desde que es medianoche
datos["segundos_desde_medianoche"] = (
    datos["hora"] * 3600
    + datos["minuto"] * 60
    + datos["segundo"]
)

# cols= [
#     "click_time",
#     "anio",
#     "mes",
#     "dia_del_mes",
#     "dia_semana",
#     "nombre_dia_semana",
#     "hora",
#     "minuto",
#     "segundo",
#     "es_fin_de_semana",
#     "es_noche",
#     "franja_horaria",
#     "segundos_desde_medianoche"
# ]

# print(datos[cols].head())

# Orden cronologico
cols_clave = ["ip", "app", "device", "os", "channel"]

# Mergesort: si hay empates, se respeta el orden previo
datos = datos.sort_values(cols_clave + ["click_time"], kind="mergesort").reset_index(drop=True)

# Indice temporal
datos["indice_temporal_global"] = datos.index

# Mismo "usuario"/grupo es ordenado
datos["posicion_en_grupo"] = (
    datos
    .groupby(cols_clave, sort=False)
    .cumcount()
    .astype("int64")
)

# tamanho del grupo
datos["total_en_grupo"] = (
    datos
    .groupby(cols_clave, sort=False)["click_time"]
    .transform("size")
    .astype("int64")
)

# check
# click_time_previo = (
#     datos
#     .groupby(cols_clave, sort=False)["click_time"]
#     .shift(1)
# )

# comparacion = (datos["click_time"] >= click_time_previo)
# comparacion = comparacion.fillna(True)
# datos["orden_monotonico_en_grupo"] = comparacion

# cols_a_ver = [
#     "ip", "app", "device", "os", "channel",
#     "click_time",
#     "indice_temporal_global",
#     "posicion_en_grupo",
#     "total_en_grupo",
#     "orden_monotonico_en_grupo"
# ]
# print(datos[cols_a_ver])

# Tiempo del clic previo y siguiente por grupo 
# instante_prev_por_grupo = (
#     datos
#     .groupby(cols_clave, sort=False)["click_time"]
#     .shift(1)
# )
# instante_next_por_grupo = (
#     datos
#     .groupby(cols_clave, sort=False)["click_time"]
#     .shift(-1)
# )

# # -1 cuando no existe previo/siguiente.
# diferencia_prev = (datos["click_time"] - instante_prev_por_grupo).dt.total_seconds()
# diferencia_next = (instante_next_por_grupo - datos["click_time"]).dt.total_seconds()

# diferencia_prev = diferencia_prev.fillna(-1)
# diferencia_next = diferencia_next.fillna(-1)

# datos["segundos_desde_clic_previo"] = diferencia_prev.astype("float64")
# datos["segundos_hasta_clic_siguiente"] = diferencia_next.astype("float64")

# print(datos.head(10))
# print(datos.columns)


# Definir columnas
cols_features = [
    "ip", "app", "device", "os", "channel",
    "anio", "mes", "dia_del_mes",
    "hora", "minuto", "segundo",
    "dia_semana", "es_fin_de_semana", "es_noche"
]
col_objetivo = "is_attributed"

dataset_entrenamiento = datos[cols_features + [col_objetivo]]

dataset_entrenamiento.to_parquet("data/entrenamiento.parquet", index=False)
