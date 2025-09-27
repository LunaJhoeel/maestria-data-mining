import pandas as pd
from evidently import Report, Dataset, DataDefinition
from evidently.presets import DataSummaryPreset

# 1. Cargar datos
df_entrenamiento = pd.read_csv("data/train_sample.csv")

# 2. Crear definición vacía (sin argumentos)
definicion = DataDefinition()

# 3. Crear Dataset a partir del DataFrame + definición
dataset_eval = Dataset.from_pandas(
    df_entrenamiento,
    data_definition=definicion,
    # puedes usar aliases para roles si lo prefieres:
    # target="is_attributed",
    # datetime_features=["click_time", "attributed_time"]
)

# 4. Crear el reporte
reporte = Report([DataSummaryPreset()])

# 5. Ejecutar reporte — devuelve snapshot
resultado = reporte.run(current_data=dataset_eval, reference_data=None)

# 6. Guardar HTML
resultado.save_html("reporte_calidad_train_sample.html")
print("Reporte generado: reporte_calidad_train_sample.html")
