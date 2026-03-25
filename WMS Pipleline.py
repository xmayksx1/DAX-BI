import pyodbc
import pandas as pd 
import os
from datetime import datetime

# 1. Configuración de Fecha para el nombre del archivo
fecha = datetime.now().strftime("%d-%m-%Y") 

# Conexión
try:
    conexion = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=172.18.1.54;'
        'DATABASE=INTELLIPICKER_PROD;'  
        'UID=CUBOS;'
        'PWD=#cUb05@c3r3@1'
    )

    # 2. Consulta SQL
    query = """
    SELECT 
        UBICA_WMS AS UBIC, 
        DESCRIP_UBICA_WMS AS UBICACION, 
        DESC_CAT_SUBLINEA AS SUBLINEA, 
        ARTICULO AS ITMCODIGO, 
        DESCRIP_ART AS ITMDESCRIPCION, 
        LOTE, 
        CANTIDAD, 
        FECHA_INGRESO, 
        FECHA_VENCE, 
        CANTIDAD AS UNIDADES, 
        CANT_CJA AS CAJAS, 
        CANT_PALLETS AS PALLETS, 
        BODEGA
    FROM V001_Consulta_Inventario_Linea
    """

    # 3. Configuración de Ruta de guardado
    ruta_destino = r"C:\Users\Usuario\OneDrive - Aceitera El Real\Escritorio\Planificación - Planificación y Control\35. Dash Temporal 2026\WMS_Historico"
    
    if not os.path.exists(ruta_destino):
        os.makedirs(ruta_destino)

    nombre_archivo = f"WMS_{fecha}.csv"
    ruta_completa = os.path.join(ruta_destino, nombre_archivo)

    print("Extrayendo datos de SQL Server...")
    
    # Carga de datos
    df = pd.read_sql(query, conexion)
 # Usamos datetime.now() para obtener fecha y hora actual en cada fila
    df.insert(0, 'FECHA_CONSULTA', datetime.now().strftime("%d-%m-%Y") )

    # 4. Exportación
    df.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
    
    print(f"¡Éxito! El archivo ha sido exportado en: {ruta_completa}")
    print(df.head())

except Exception as e:
    print(f"Error al procesar la consulta o exportar: {e}")

finally:
    if 'conexion' in locals():
        conexion.close()
        print("Conexión cerrada.")