"""
Crea una tabla Delta LOCAL de bronze para el CI, a partir de ci/bronze_muestra.csv
(una muestra REAL extraída de nuestro bronze: nov-2025 a feb-2026, cruza el cambio de fuente).

En CI no hay MinIO: dbt lee esta tabla con el mismo source y el mismo plugin delta que en
producción, solo cambia BRONZE_PATH. Así el CI prueba exactamente el SQL que corre en el servidor.

Uso: python ci/crear_bronze_muestra.py [csv] [destino]
"""
import shutil
import sys

import pandas as pd
from deltalake import write_deltalake

origen = sys.argv[1] if len(sys.argv) > 1 else "ci/bronze_muestra.csv"
destino = sys.argv[2] if len(sys.argv) > 2 else "ci/bronze_delta"

df = pd.read_csv(origen, dtype={"fecha": str, "fuente": str, "ingestado_en": str})
num = [c for c in df.columns if c not in ("fecha", "fuente", "ingestado_en")]
df[num] = df[num].astype("float64")
shutil.rmtree(destino, ignore_errors=True)
write_deltalake(destino, df, mode="overwrite")
print(f"Bronze de muestra: {len(df)} filas ({df.fecha.min()} -> {df.fecha.max()}) en {destino}")
