import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# --- FUNCIÓN MAESTRA DE LIMPIEZA Y VALIDACIÓN ---
def procesar_dataset(url, nombre_tabla, columnas_obligatorias, db_url=None):
    print(f"\n>>> Iniciando proceso para: {nombre_tabla} <<<")
    
    # 1. Carga
    df = pd.read_csv(url)
    
    # 2. Normalización de encabezados
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
    
    # 3. Limpieza de texto y nulos
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace(["nan", "None", "NULL", "null", ""], np.nan)
    
    # 4. Transformaciones automáticas (Fechas y Números)
    for col in df.columns:
        if 'fecha' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        if col in ['costo', 'monto', 'precio', 'edad']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 5. Separación Curated / Rejects
    # Filtramos: si alguna de las columnas obligatorias es NaN, va a Rejects
    mask_validos = df[columnas_obligatorias].notna().all(axis=1)
    
    df_curated = df[mask_validos].copy()
    df_rejects = df[~mask_validos].copy()
    
    # 6. Identificación de Motivos de Rechazo
    def obtener_motivos(row):
        fallas = [col for col in columnas_obligatorias if pd.isna(row[col])]
        return ",".join([f"{c}_vacio" for c in fallas])
    
    if not df_rejects.empty:
        df_rejects["motivo_rechazo"] = df_rejects.apply(obtener_motivos, axis=1)
    
    # 7. Exportación a CSV (Para el repositorio)
    df_curated.to_csv(f"{nombre_tabla}_curated.csv", index=False)
    df_rejects.to_csv(f"{nombre_tabla}_rejects.csv", index=False)
    
    # 8. Carga a Base de Datos (Opcional)
    if db_url:
        try:
            engine = create_engine(db_url)
            df_curated.to_sql(f"{nombre_tabla}_curated", engine, if_exists='replace', index=False)
            df_rejects.to_sql(f"{nombre_tabla}_rejects", engine, if_exists='replace', index=False)
            print(f"✓ Carga exitosa en Render para {nombre_tabla}")
        except Exception as e:
            print(f"x Error en carga de {nombre_tabla}: {e}")
            
    print(f"✓ Archivos CSV generados: {nombre_tabla}_curated.csv y {nombre_tabla}_rejects.csv")
    print(f"✓ Resumen: {len(df_curated)} válidos, {len(df_rejects)} rechazados.")
    return df_curated, df_rejects

# CONEXION DB
URL_CONEXION = "postgresql+psycopg2://usuario:password@host:5432/basedatos"

# Ejemplo de uso para Pacientes
url_p = "https://raw.githubusercontent.com/gaby1719/datawerehouse-1719312021/refs/heads/main/raw/Z_pacientes%202(in).csv"
pacientes_c, pacientes_r = procesar_dataset(url_p, "pacientes", ["id_paciente", "nombre", "correo"], URL_CONEXION)

# Ejemplo de uso para Consultas
url_c = "https://raw.githubusercontent.com/gaby1719/datawerehouse-1719312021/refs/heads/main/raw/Z_consultas%202(in).csv"
consultas_c, consultas_r = procesar_dataset(url_c, "consultas", ["id_consulta", "id_paciente"], URL_CONEXION)

# Ejemplo de uso para Tratamientos
url_t = "https://raw.githubusercontent.com/gaby1719/datawerehouse-1719312021/refs/heads/main/raw/Z_tratamientos%202(in).csv"
tratamientos_c, tratamientos_r = procesar_dataset(url_t, "tratamientos", ["id_tratamiento", "id_consulta"], URL_CONEXION)