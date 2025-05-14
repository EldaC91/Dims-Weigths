# Librerias a usar
import shutil
from datetime import datetime
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, inspect, NVARCHAR, Float, Date, text
from sqlalchemy import text
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from prophet import Prophet
from pmdarima import auto_arima
from tqdm import tqdm 
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import lightgbm as lgb
from sklearn.preprocessing import OrdinalEncoder
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score
import sqlalchemy
from sqlalchemy import create_engine, types
import pyodbc
import smtplib
from email.mime.text import MIMEText
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String, Float
import os
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
def send_email_inicio():
    # Configuración del correo
    sender_email = "ecalderon@maxwarehouse.com"  
    sender_password = "Maxwarehouse2025$"  

    # 🔹 Lista de destinatarios
    recipient_list = ["ecalderon@maxwarehouse.com", "vpverbena@maxwarehouse.com"]
    
    # Contenido del correo
    subject = "🔔 Alerta: Inicio de generación de predicciones de pesos"
    body = "El modelo de predicción ha empezado a correr."

    # Crear el mensaje
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_list)

    try:
        # Conectar al servidor SMTP (ejemplo para Gmail)
        server = smtplib.SMTP("smtp.office365.com", 587)  # Usa "smtp.office365.com" para Outlook
        server.starttls()  # Seguridad TLS
        server.login(sender_email, sender_password)  # Autenticación
        server.sendmail(sender_email, recipient_list, msg.as_string())  # Enviar
        server.quit()
        print("✅ Correo enviado correctamente")
    except Exception as e:
        print(f"⚠ Error al enviar el correo: {e}")

# Llamar a la función al final del proceso
send_email_inicio()
# --- Configuración ---

# Carpeta donde están los archivos Excel
carpeta = r'C:\Users\ecalderon\OneDrive - BITS - Max Warehouse\Shipping\Weights Process\Reporte Semanal Endicia'
carpeta_destino = r'C:\Users\ecalderon\OneDrive - BITS - Max Warehouse\Shipping\Weights Process\Reporte Semanal Endicia\Procesados'

# Listar todos los archivos .xlsx de la carpeta
excel_files = [f for f in os.listdir(carpeta) if f.endswith('.xlsx')]

# Datos de conexión a SQL Server
server = '10.12.200.59' 
database = 'Maxwarehouse'
table_name = 'endicia_semanal'

# sqlalchemy + pyodbc
connection_string = f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
engine = create_engine(connection_string)

# Verificar si hay archivos para procesar
if excel_files:
    for excel_file in excel_files:
        try:
            # Ruta completa al archivo
            ruta_archivo = os.path.join(carpeta, excel_file)

            # Leer el archivo Excel
            df = pd.read_excel(ruta_archivo)
            print(f"Archivo leído: {excel_file}")

            # Cargar los datos a SQL Server
            with engine.connect() as connection:
                df.to_sql(table_name, con=connection, if_exists='append', index=False)

            print(f"Datos de {excel_file} cargados exitosamente a SQL Server 🎯")

            # --- Mover y renombrar el archivo ---
            # Obtener la fecha de hoy
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')

            # Armar el nuevo nombre
            nombre_base, extension = os.path.splitext(excel_file)
            nuevo_nombre = f'{nombre_base}_{fecha_hoy}{extension}'

            # Ruta destino con el nuevo nombre
            ruta_destino = os.path.join(carpeta_destino, nuevo_nombre)

            # Mover y renombrar
            shutil.move(ruta_archivo, ruta_destino)
            print(f"Archivo {excel_file} movido y renombrado a: {ruta_destino}")

        except Exception as e:
            print(f"⚠️ Error procesando {excel_file}: {e}")

else:
    print("No se encontraron archivos Excel (.xlsx) en la carpeta. Continuando con el resto del código.")

server = '10.12.200.59' #ip del DB es 10.11.100.103
database = 'Maxwarehouse'

# Crear el engine usando la cadena de conexión para SQL Server con pyodbc
engine = create_engine(f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")

# Ejecutar consulta 

query = """

SELECT TrackingNumber, ParentSku, SalesStatusEVP, TotalSales, TotalCost FROM [DWH-MaxWarehouse].[Fact].[Sales]
WHERE SalesOrderDateTime >= DATEADD(MONTH, -4, CAST(GETDATE() AS DATE)) 

"""

df_ventas = pd.read_sql(query, con=engine)

server = '10.12.200.59' #ip del DB es 10.11.100.103
database = 'Maxwarehouse'

# Crear el engine usando la cadena de conexión para SQL Server con pyodbc
engine = create_engine(f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")

# Ejecutar consulta 

query = """

WITH BOXFECHAS AS (
    SELECT 
        TRACKING_NUMBER,
        OrderDate,
        WEIGHT AS WeightBox,
        CASE WHEN LEN(DIMENSIONS) >= 5 THEN CAST(SUBSTRING(DIMENSIONS, 1, CHARINDEX('x', DIMENSIONS) - 1) AS FLOAT) ELSE 0.00 END AS DimHeightBOX,
        CASE WHEN LEN(DIMENSIONS) >= 5 THEN CAST(SUBSTRING(DIMENSIONS, CHARINDEX('x', DIMENSIONS) + 1, CHARINDEX('x', DIMENSIONS, CHARINDEX('x', DIMENSIONS) + 1) - CHARINDEX('x', DIMENSIONS) - 1) AS FLOAT) ELSE 0.00 END AS DimLengthBOX,
        CASE WHEN LEN(DIMENSIONS) >= 5 THEN CAST(RIGHT(DIMENSIONS, LEN(DIMENSIONS) - CHARINDEX('x', DIMENSIONS, CHARINDEX('x', DIMENSIONS) + 1)) AS FLOAT) ELSE 0.00 END AS DimWidthBOX,
        CCN_ORDER_NUMBER
    FROM BOX
    WHERE ISDATE(OrderDate) = 1
),

endicia_semana_mes AS (
    SELECT DISTINCT  
        [Postmark_Date] AS TranDate, 
        [Transaction_Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
        [Weight_(lb)], 
        [Package Height], 
        [Package Length], 
        [Package Width]
    FROM Endiciatransacprovision
    WHERE ISDATE([Transaction_Date (GMT)]) = 1
      AND CAST([Transaction_Date (GMT)] AS date) >= DATEADD(MONTH, -4, GETDATE())
    
    UNION ALL
    
    SELECT DISTINCT  
        [Postmark Date] AS TranDate, 
        [Transaction Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
        [Weight (lb)], 
        [Package Height], 
        [Package Length], 
        [Package Width]
    FROM endicia_semanal
    WHERE ISDATE([Transaction Date (GMT)]) = 1
      AND CAST([Transaction Date (GMT)] AS date) >= DATEADD(MONTH, -2, GETDATE())
),

Factura3meses AS (

    -- FEDEX
    SELECT DISTINCT
        CAST(InvoiceDate AS date) AS TranDate,
        CAST(InvoiceDate AS date) AS InvDate,
        GroundTrackingIDPrefix + ExpressorGroundTrackingID AS trackingnum,
        MAX(ActualWeightAmount) AS max_weight, 
        COALESCE(NULLIF(DimHeight, ''), CAST(DimHeightBOX AS VARCHAR)) AS DimHeight,
        COALESCE(NULLIF(DimLength, ''), CAST(DimLengthBOX AS VARCHAR)) AS DimLength,
        COALESCE(NULLIF(DimWidth, ''), CAST(DimWidthBOX AS VARCHAR)) AS DimWidth,
        MAX(CCN_ORDER_NUMBER) AS CCN_ORDER_NUMBER
    FROM 
        FEDEXHISTORICO
    LEFT JOIN 
        BOXFECHAS ON GroundTrackingIDPrefix + ExpressorGroundTrackingID = TRACKING_NUMBER
    WHERE 
        CAST(InvoiceDate AS date) >= DATEADD(MONTH, -3, GETDATE())
    GROUP BY 
        CAST(InvoiceDate AS date),
        CAST(InvoiceDate AS date),
        GroundTrackingIDPrefix + ExpressorGroundTrackingID,
        COALESCE(NULLIF(DimHeight, ''), CAST(DimHeightBOX AS VARCHAR)),
        COALESCE(NULLIF(DimLength, ''), CAST(DimLengthBOX AS VARCHAR)),
        COALESCE(NULLIF(DimWidth, ''), CAST(DimWidthBOX AS VARCHAR))

    UNION ALL

    -- UPS (CON PRIORIDAD PackageDimensions)
    SELECT 
    TranDate,
    InvDate,
    TrackingNumber,
    max_weight,
    Valor1 AS DimHeight,
    Valor2 AS DimLength,
    Valor3 AS DimWidth,
    CCN_ORDER_NUMBER
	FROM (
    SELECT 
        CAST(u.TransactionDate AS date) AS TranDate,
        CAST(u.InvoiceDate AS date) AS InvDate,
        u.TrackingNumber,
        CASE 
            WHEN u.EnteredWeight != 0 THEN u.EnteredWeight
            WHEN u.BilledWeight IS NOT NULL AND u.BilledWeight != 0 THEN u.BilledWeight
            ELSE b.WeightBox
        END AS max_weight,
        CASE 
            WHEN COALESCE(u.PackageDimensions, '') <> '' THEN CAST(SUBSTRING(u.PackageDimensions, 1, CHARINDEX('x', u.PackageDimensions) - 1) AS FLOAT)
            WHEN LEN(u.DetailKeyedDim) >= 5 THEN CAST(SUBSTRING(u.DetailKeyedDim, 1, CHARINDEX('x', u.DetailKeyedDim) - 1) AS FLOAT)
            ELSE b.DimHeightBOX
        END AS Valor1,
        CASE 
            WHEN COALESCE(u.PackageDimensions, '') <> '' THEN CAST(SUBSTRING(u.PackageDimensions, CHARINDEX('x', u.PackageDimensions) + 1, CHARINDEX('x', u.PackageDimensions, CHARINDEX('x', u.PackageDimensions) + 1) - CHARINDEX('x', u.PackageDimensions) - 1) AS FLOAT)
            WHEN LEN(u.DetailKeyedDim) >= 5 THEN CAST(SUBSTRING(u.DetailKeyedDim, CHARINDEX('x', u.DetailKeyedDim) + 1, CHARINDEX('x', u.DetailKeyedDim, CHARINDEX('x', u.DetailKeyedDim) + 1) - CHARINDEX('x', u.DetailKeyedDim) - 1) AS FLOAT)
            ELSE b.DimLengthBOX
        END AS Valor2,
        CASE 
            WHEN COALESCE(u.PackageDimensions, '') <> '' THEN CAST(RIGHT(u.PackageDimensions, LEN(u.PackageDimensions) - CHARINDEX('x', u.PackageDimensions, CHARINDEX('x', u.PackageDimensions) + 1)) AS FLOAT)
            WHEN LEN(u.DetailKeyedDim) >= 5 THEN CAST(RIGHT(u.DetailKeyedDim, LEN(u.DetailKeyedDim) - CHARINDEX('x', u.DetailKeyedDim, CHARINDEX('x', u.DetailKeyedDim) + 1)) AS FLOAT)
            ELSE b.DimWidthBOX
        END AS Valor3,
        CCN_ORDER_NUMBER,
        ROW_NUMBER() OVER (
            PARTITION BY u.TrackingNumber
            ORDER BY 
                CASE 
                    WHEN COALESCE(u.PackageDimensions, '') <> '' THEN 1
                    WHEN LEN(u.DetailKeyedDim) >= 5 THEN 2
                    ELSE 3
                END ASC,
                u.InvoiceDate DESC
        ) AS RowNum
    FROM 
        UPSNEWFORMAT u
    LEFT JOIN 
        BOXFECHAS b ON u.TrackingNumber = b.TRACKING_NUMBER
    WHERE 
        CAST(u.InvoiceDate AS date) >= DATEADD(MONTH, -3, GETDATE())
	) AS UPSFiltrado
	WHERE RowNum = 1


    UNION ALL

    -- USPS (Endicia)
    SELECT DISTINCT 
        TranDate,
        InvDate,
        trackingnum,
        COALESCE([Weight_(lb)], b.WeightBox) AS max_weight,
        COALESCE([Package Height], b.DimHeightBOX) AS DimHeight,
        COALESCE([Package Length], b.DimLengthBOX) AS DimLength,
        COALESCE([Package Width], b.DimWidthBOX) AS DimWidth,
        CCN_ORDER_NUMBER
    FROM 
        endicia_semana_mes e
    LEFT JOIN 
        BOXFECHAS b ON b.TRACKING_NUMBER = e.trackingnum
    WHERE 
        trackingnum IS NOT NULL
        AND CAST(InvDate AS date) >= DATEADD(MONTH, -4, GETDATE()) -- USPS 4 meses porque vienen mensual
),

DWVentasUnicas as (
	Select 
		SalesOrderNumber, Sku
	from DWShippingInfo as dws
	where cast(SalesOrderDate as date ) between dateadd(MONTH,-4, cast(GETDATE() as date)) and cast(GETDATE() as date)
	group by SalesOrderNumber, Sku
),

CantidadSkusxVenta as (
	select
		SalesOrderNumber, count(Sku) CantidadSkus
	from DWVentasUnicas dw 
	group by SalesOrderNumber
	having count(Sku) =1
),

PesosfacturaVentas as (
	--Lista los trackings con su peso y le coloca la moda y las frecuencia con que ese peso fue facturado
	select  
		TranDate, InvDate, dw.trackingnum, max_weight, DimHeight, DimLength, DimWidth, mw.Sku, mw.UomCode, mw.UomQuantity
	FROM Factura3meses dw
	INNER JOIN DWShippingInfo  mw
		ON dw.trackingnum = mw.trackingnum
	WHERE dw.trackingnum !=''
	and SalesOrderNumber in (select SalesOrderNumber from CantidadSkusxVenta)
	and cast(UomQuantity as float)/cast(Quantity as float)= 1
),

RankedData AS (
    SELECT 
        TranDate, 
        InvDate, 
        trackingnum, 
        Sku, 
        UomCode, 
        UomQuantity,
        max_weight, 
        DimHeight, 
        DimLength, 
        DimWidth,
        RANK() OVER (
            PARTITION BY TranDate, trackingnum, Sku, UomCode 
            ORDER BY InvDate DESC
        ) AS Ranking
    FROM PesosfacturaVentas
),

Max_Rank as (SELECT 
    TranDate, 
    InvDate, 
    trackingnum, 
    Sku, 
    UomCode, 
    UomQuantity,
    MAX(max_weight) AS Weight, 
    MAX(DimHeight) AS Height, 
    MAX(DimLength) AS Length, 
    MAX(DimWidth) AS Width,
    Ranking
FROM RankedData
GROUP BY TranDate, InvDate, trackingnum, Sku, UomCode, UomQuantity, Ranking
),

EJD_DIMS as (SELECT aa.*, 
       bb.[EJD Weight], 
       bb.[EJD Height], 
       bb.[EJD Length], 
       bb.[EJD Width],
       bb.[BaseUnit]
FROM Max_Rank aa
LEFT JOIN (
    SELECT DISTINCT [EVP SKU], [EVP UOM CODE], [EVP UOM QTY], [EJD Weight], [EJD Height], [EJD Length], [EJD Width], [BaseUnit]
    FROM [stg-MaxWarehouse].[EvpCsv].[EVPAlllSKUsDimNWeight]
) bb
ON aa.Sku COLLATE SQL_Latin1_General_CP1_CI_AS = bb.[EVP SKU] COLLATE SQL_Latin1_General_CP1_CI_AS
and aa.UomCode COLLATE SQL_Latin1_General_CP1_CI_AS= bb.[EVP UOM CODE] COLLATE SQL_Latin1_General_CP1_CI_AS
),

Ventas as (
    SELECT * 
    FROM [DWH-MaxWarehouse].[Fact].[Sales]
    WHERE SalesOrderDateTime >= DATEADD(MONTH, -4, CAST(GETDATE() AS DATETIME))
      AND SalesOrderDateTime < DATEADD(DAY, 1, CAST(GETDATE() AS DATETIME))
)

select distinct * from EJD_DIMS 

"""

q1 = pd.read_sql(query, con=engine)
df_merged = pd.merge(q1, df_ventas, left_on=['trackingnum', 'Sku'], right_on=['TrackingNumber', 'ParentSku'], how='left')
df = df_merged.copy()
df = df.rename(columns={
    "EJD Weight": "EJD_Weight",
    "EJD Height": "EJD_Height",
    "EJD Length": "EJD_Length",
    "EJD Width": "EJD_Width"
})
def normalize_uom_code(df, column='UomCode'):
    """
    Normaliza la columna UomCode:
    - Convierte a mayúsculas
    - Elimina espacios innecesarios
    - Reemplaza valores según las reglas definidas
    - Convierte formatos como "CASE_X" → "CS/X" y "PACK OF X" → "PK/X"
    
    Parámetros:
        df (pd.DataFrame): DataFrame con la columna UomCode
        column (str): Nombre de la columna a normalizar
    
    Retorna:
        pd.DataFrame: DataFrame con una nueva columna 'UomCode_N' normalizada
    """
    df = df.copy()
    
    # Convertir a mayúsculas y eliminar espacios innecesarios
    df['UomCode_N'] = df[column].str.strip().str.upper()

    # Reemplazar "CASE_X" → "CS/X" antes de otros reemplazos
    df['UomCode_N'] = df['UomCode_N'].str.replace(r'^CASE[_\s]?(\d+)', r'CS/\1', regex=True)

    # Reemplazar caracteres no uniformes y homogenizar
    replacements = {
        r'\bEACH\b': 'EA',  # EACH → EA
        r'\bEA\b': 'EA',    # Variaciones de EA → EA
        r'\bCASE\b': 'CS',  # CASE → CS
        r'\bPACK\b': 'PK',  # PACK → PK
        r'_' : '/'         # Reemplazar "_" por "/"
    }
    
    df['UomCode_N'] = df['UomCode_N'].replace(replacements, regex=True)

    # Convertir "PACK/X" a "PK/X"
    df['UomCode_N'] = df['UomCode_N'].str.replace(r'^PACK/', 'PK/', regex=True)
    df['UomCode_N'] = df['UomCode_N'].str.replace(r'^PA/', 'PK/', regex=True)

    # Convertir "CS OF X" → "CS_X" y "PK OF X" → "PK_X"
    df['UomCode_N'] = df['UomCode_N'].str.replace(r'^CS OF (\d+)', r'CS/\1', regex=True)
    df['UomCode_N'] = df['UomCode_N'].str.replace(r'^PK OF (\d+)', r'PK/\1', regex=True)

    # Reemplazar '/' por '_'
    df['UomCode_N'] = df['UomCode_N'].str.replace('/', '_', regex=False)

    # Reemplazar "ACE" por "EA" y quitar el resto de la cadena
    df['UomCode_N'] = df['UomCode_N'].str.replace(r'^ACE.*', 'EA', regex=True)

    return df
df = normalize_uom_code(df)
# Asegurar que las fechas son tipo datetime
df['TranDate'] = pd.to_datetime(df['TranDate'])
df['InvDate'] = pd.to_datetime(df['InvDate'])

# Ordenar primero 
df = df.sort_values(['TranDate', 'trackingnum', 'Sku', 'UomCode_N', 'InvDate'], ascending=[True, True, True, True, False])

# Obtener el índice del invdate más reciente dentro de cada grupo
idx = df.groupby(['TranDate', 'trackingnum', 'Sku', 'UomCode_N'])['InvDate'].idxmax()

# Seleccionar las filas correspondientes
df_mas_reciente = df.loc[idx].reset_index(drop=True)
# Asegurar que las fechas son tipo datetime

# Ordenar primero 
df_mas_reciente2 = df_mas_reciente.sort_values(['TranDate', 'trackingnum', 'Sku', 'UomCode_N', 'InvDate'], ascending=[False, True, True, True, True])

# Obtener el índice del invdate más reciente dentro de cada grupo
idx = df_mas_reciente2.groupby(['InvDate', 'trackingnum', 'Sku', 'UomCode_N'])['TranDate'].idxmax()

# Seleccionar las filas correspondientes
df_mas_reciente2 = df_mas_reciente2.loc[idx].reset_index(drop=True)
# Eliminar trackings repetidos por fechas

# Ordenar y seleccionar la fila con invdate más reciente para cada grupo de ['trackingnum', 'Sku', 'UomCode_N']
idx = df_mas_reciente2.groupby(['trackingnum', 'Sku', 'UomCode_N'])['InvDate'].idxmax()

# 3. Filtrar las filas
df_mas_reciente3 = df_mas_reciente2.loc[idx].reset_index(drop=True)

df = df_mas_reciente3
df2 = df.sort_values(by=['TranDate', 'trackingnum', 'Sku', 'UomCode_N', 'InvDate'], ascending=[True, True, True, True, False])

cols_to_fill = ['Weight', 'Height', 'Length', 'Width']
df2[cols_to_fill] = df2[cols_to_fill].replace(0, pd.NA)

df2[cols_to_fill] = df2.groupby(['TranDate', 'trackingnum', 'Sku', 'UomCode_N'])[cols_to_fill].transform(lambda x: x.bfill())
# Ordenar por TranDate, trackingnum, Sku, UomCode_N, y InvDate DESC
df2 = df2.sort_values(by=['TranDate', 'trackingnum', 'Sku', 'UomCode_N', 'InvDate'], ascending=[True, True, True, True, False])

# Asignar ranking dentro de cada grupo según InvDate DESC (el primero será Rank 1)
df2['Rank'] = df2.groupby(['TranDate', 'trackingnum', 'Sku', 'UomCode_N']).cumcount() + 1

# Filtrar solo los de Ranking = 1
df_final = df2[df2['Rank'] == 1]
# Weight con nulos relleno con la moda

# Función para obtener la moda, con manejo de múltiples modas
def obtener_moda(grupo):
    moda = grupo.mode()
    if not moda.empty:
        return moda.iloc[0]  # toma la primera si hay varias
    else:
        return np.nan

# Calcular la moda por combinación de Sku y UomCode_N
moda_por_sku_uom = (
    df_final.groupby(['Sku', 'UomCode_N'])['Weight']
    .transform(lambda x: x.fillna(obtener_moda(x)))
)

# Reemplazar los valores nulos en Weight con la moda correspondiente
df_final['Weight'] = df_final['Weight'].fillna(moda_por_sku_uom)
# Eliminar nulos

df_final = df_final.dropna(subset=['TranDate', 'InvDate', 'trackingnum', 'Sku', 'UomCode_N', 'Weight', 'Weight', 'Length', 'Width'])
df_final.fillna(0, inplace=True)
#Convertir tipo de dato

df_final['Weight'] = df_final['Weight'].astype(str).astype(float)
df_final['Height'] = df_final['Height'].astype(str).astype(float)
df_final['Length'] = df_final['Length'].astype(str).astype(float)
df_final['Width'] = df_final['Width'].astype(str).astype(float)
df_final['EJD_Weight'] = df_final['EJD_Weight'].astype(str).astype(float)
df_final['EJD_Length'] = df_final['EJD_Length'].astype(str).astype(float)
df_final['EJD_Height'] = df_final['EJD_Height'].astype(str).astype(float)
df_final['EJD_Width'] = df_final['EJD_Width'].astype(str).astype(float)
df_final['TranDate'] = pd.to_datetime(df_final['TranDate'], format='%d-%m-%Y')
df_final['InvDate'] = pd.to_datetime(df_final['InvDate'], format='%d-%m-%Y')
df_final['UomQuantity'] = df_final['UomQuantity'].astype(str).astype(float).round(1)
# Ordenar Dimensiones de mayor a menor
df_sorted = df_final.copy()
df_sorted[['Height', 'Length', 'Width']] = df_final[['Height', 'Length', 'Width']].apply(
    lambda row: sorted(row, reverse=True), axis=1, result_type='expand')

df_sorted[['EJD_Height', 'EJD_Length', 'EJD_Width']] = df_final[['EJD_Height', 'EJD_Length', 'EJD_Width']].apply(
    lambda row: sorted(row, reverse=True), axis=1, result_type='expand')
df_sorted2 = df_sorted[['trackingnum', 'Sku', 'UomCode', 'Weight', 'Height', 'Length', 'Width']]
df_sorted2.rename(columns={
    'Weight': 'Weight_Original',
    'Height': 'Height_Original',
    'Length': 'Length_Original',
    'Width': 'Width_Original'
}, inplace=True)
# Función para reemplazar outliers por NaN por Sku usando IQR
def replace_outliers_with_nan(group):
    Q1 = group[['Weight', 'Height', 'Length', 'Width']].quantile(0.25)
    Q3 = group[['Weight', 'Height', 'Length', 'Width']].quantile(0.75)
    IQR = Q3 - Q1
    factor = 1.5

    # Detectar outliers
    is_outlier = ((group[['Weight', 'Height', 'Length', 'Width']] < (Q1 - factor * IQR)) |
                  (group[['Weight', 'Height', 'Length', 'Width']] > (Q3 + factor * IQR)))

    # Reemplazar outliers por NaN
    group[['Weight', 'Height', 'Length', 'Width']] = group[['Weight', 'Height', 'Length', 'Width']].mask(is_outlier, np.nan)

    return group

# Aplicar la función por cada Sku
df_cleaned = df_sorted.groupby(['Sku', 'UomCode_N']).apply(replace_outliers_with_nan)

# Reiniciar el índice si deseas (opcional)
df_cleaned = df_cleaned.reset_index(drop=True)
# Rellenar Vacios de outliers con la mediana
df_cleaned[['Weight', 'Height', 'Length', 'Width']] = df_cleaned.groupby(['Sku', 'UomCode_N'])[['Weight', 'Height', 'Length', 'Width']].transform(lambda x: x.fillna(x.median()))
df = df_cleaned.copy()
# Asegurar que dimensiones son numéricas
df[['Height', 'Length', 'Width']] = df[['Height', 'Length', 'Width']].astype(float)

# Definir tolerancia
tolerancia = 2

# Agrupar por SKU y UomCode y aplicar "agrupación por bins"
df['dim_group'] = (
    df.groupby(['Sku', 'UomCode'], group_keys=False)
      .apply(lambda g: (
          ((g[['Height', 'Length', 'Width']] // tolerancia)
           .astype(int)
           .astype(str)
           .agg('_'.join, axis=1))
      ))
)

# Ahora convertir esas combinaciones a códigos únicos (grupos numéricos)
df['grupo_dim'] = df.groupby(['Sku', 'UomCode', 'dim_group'], group_keys=False).ngroup()

# Opcional: eliminar columna temporal
df.drop(columns='dim_group', inplace=True)
# Agrupamos y tomamos el máximo por Sku, UomCode y grupo_dim
dimensiones_max = df.groupby(['Sku', 'UomCode', 'grupo_dim']).agg({
    'Height': 'max',
    'Length': 'max',
    'Width': 'max',
    'Weight': 'mean',
    'trackingnum': 'count'  # para saber cuántas veces aparece ese grupo
}).rename(columns={'trackingnum': 'n'})

# Agregar columna con criterio de desempate
dimensiones_max['criterio'] = (
    dimensiones_max['Height'] + 2 * dimensiones_max['Length'] + 2 * dimensiones_max['Width']
)

# Ordenar por frecuencia y criterio
dimensiones_final = (
    dimensiones_max
    .reset_index()
    .sort_values(['Sku', 'UomCode', 'n', 'criterio'], ascending=[True, True, False, False])
    .groupby(['Sku', 'UomCode'])
    .first()
    .reset_index()
)

# Redondear dimensiones a 0 decimales
dimensiones_final[['Height', 'Length', 'Width']] = (
    dimensiones_final[['Height', 'Length', 'Width']].round(0)
)

# Redondear peso a 2 decimales
dimensiones_final['Weight'] = (dimensiones_final[ 'Weight'].round(2))
df_supplier = df_cleaned[["Sku", "UomCode", "UomQuantity", "BaseUnit", "EJD_Weight", "EJD_Height", "EJD_Length", "EJD_Width"]].drop_duplicates()
df_supplier = df_supplier.groupby(["Sku", "UomCode", "UomQuantity", "BaseUnit"])[["EJD_Weight", "EJD_Height", "EJD_Length", "EJD_Width"]].max().reset_index()
df_supplier["BaseUnit"] = df_supplier["BaseUnit"].astype(str).str.lower().map({"true": True, "false": False})
df_supplier = df_supplier.sort_values(by="BaseUnit", ascending=False).drop_duplicates(subset=["Sku", "UomCode"])
# Si UomQty es 1 se toma como BaseUnit ese sku

# Base predicciones
index_bu_null = df_supplier[df_supplier["BaseUnit"].isna()].index
df_supplier.loc[index_bu_null, 'BaseUnit'] = True

# Base historico
index_bu_null = df_cleaned[df_cleaned["BaseUnit"].isna()].index
df_cleaned.loc[index_bu_null, 'BaseUnit'] = True

df_supplier.loc[df_supplier[df_supplier['BaseUnit'] == True].index, 'BaseUnit'] = True
# Limpieza base cleaned
true_clean_i = df_cleaned[df_cleaned['BaseUnit'] == 'True'].index
df_cleaned.loc[true_clean_i, 'BaseUnit'] = True

false_clean_i = df_cleaned[df_cleaned['BaseUnit'] == 'False'].index
df_cleaned.loc[false_clean_i, 'BaseUnit'] = False
df_supplier_na_base = df_supplier[df_supplier["BaseUnit"].isna()]
df_cleaned_na_base = df_cleaned[df_cleaned["BaseUnit"] == 0]
final_w_supplier = pd.merge(dimensiones_final, df_supplier, on=["Sku", "UomCode"], how="left")
df_pred = final_w_supplier[final_w_supplier['BaseUnit'] == True]
df_pred = df_pred[["Sku", "UomCode", "UomQuantity", 'Weight', 'Height', 'Length', 'Width']]
final_w_supplierT = final_w_supplier[final_w_supplier['BaseUnit'] == True]
final_w_supplierF = final_w_supplier[final_w_supplier['BaseUnit'] == False]

df_cleanedT = df_cleaned[df_cleaned['BaseUnit'] == True]
df_cleanedF = df_cleaned[df_cleaned['BaseUnit'] == False]
#### PREDICCIONES
# Threshold para skus con peso menor a 1
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] < 1, "th_weight"] = final_w_supplierT["EJD_Weight"] + 0.31
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] < 1, "th_height"] = final_w_supplierT["EJD_Height"] + 3
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] < 1, "th_length"] = final_w_supplierT["EJD_Length"] + 3
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] < 1, "th_width"] = final_w_supplierT["EJD_Width"] + 3

# Threshold para skus con peso mayor a 1
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] >= 1, "th_weight"] = final_w_supplierT["EJD_Weight"] + 3
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] >= 1, "th_height"] = final_w_supplierT["EJD_Height"] + 5
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] >= 1, "th_length"] = final_w_supplierT["EJD_Length"] + 5
final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] >= 1, "th_width"] = final_w_supplierT["EJD_Width"] + 5

#### HISTORICO
# Threshold para skus con peso menor a 1
df_cleanedT.loc[df_cleanedT["EJD_Weight"] < 1, "th_weight"] = df_cleanedT["EJD_Weight"] + 0.31
df_cleanedT.loc[df_cleanedT["EJD_Weight"] < 1, "th_height"] = df_cleanedT["EJD_Height"] + 3
df_cleanedT.loc[df_cleanedT["EJD_Weight"] < 1, "th_length"] = df_cleanedT["EJD_Length"] + 3
df_cleanedT.loc[df_cleanedT["EJD_Weight"] < 1, "th_width"] = df_cleanedT["EJD_Width"] + 3

# Threshold para skus con peso mayor a 1
df_cleanedT.loc[df_cleanedT["EJD_Weight"] >= 1, "th_weight"] = df_cleanedT["EJD_Weight"] + 3
df_cleanedT.loc[df_cleanedT["EJD_Weight"] >= 1, "th_height"] = df_cleanedT["EJD_Height"] + 5
df_cleanedT.loc[df_cleanedT["EJD_Weight"] >= 1, "th_length"] = df_cleanedT["EJD_Length"] + 5
df_cleanedT.loc[df_cleanedT["EJD_Weight"] >= 1, "th_width"] = df_cleanedT["EJD_Width"] + 5
# Pesos y dimensiones finales (aplicando el threshold)

#### Sobrepasando limite superior del rango ########
final_w_supplierT.loc[final_w_supplierT["Weight"] > final_w_supplierT["th_weight"], "fail_weight"] = 1
final_w_supplierT["Final_Weight"] = final_w_supplierT["Weight"]

final_w_supplierT.loc[final_w_supplierT["Height"] > final_w_supplierT["th_height"], "fail_eight"] = 1
final_w_supplierT["Final_Height"] = final_w_supplierT["Height"]

final_w_supplierT.loc[final_w_supplierT["Length"] > final_w_supplierT["th_length"], "fail_length"] = 1
final_w_supplierT["Final_Length"] = final_w_supplierT["Length"]

final_w_supplierT.loc[final_w_supplierT["Width"] > final_w_supplierT["th_width"], "fail_width"] = 1
final_w_supplierT["Final_Width"] = final_w_supplierT["Width"]


#### Sobrepasando limite inferior del rango ########
final_w_supplierT.loc[final_w_supplierT["Weight"] < final_w_supplierT["EJD_Weight"], "fail_weight"] = 1
final_w_supplierT.loc[final_w_supplierT["Height"] < final_w_supplierT["EJD_Height"], "fail_eight"] = 1
final_w_supplierT.loc[final_w_supplierT["Length"] < final_w_supplierT["EJD_Length"], "fail_length"] = 1
final_w_supplierT.loc[final_w_supplierT["Width"] < final_w_supplierT["EJD_Width"], "fail_width"] = 1
# Pesos y dimensiones finales (aplicando el threshold)

#### Skus que no tienen dimensiones en EJD ########

final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] == 0, "fail_weight"] = np.nan
final_w_supplierF["Final_Weight"] = final_w_supplierF["Weight"]

final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] == 0, "fail_eight"] = np.nan
final_w_supplierF["Final_Height"] = final_w_supplierF["Height"]

final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] == 0, "fail_length"] = np.nan
final_w_supplierF["Final_Length"] = final_w_supplierF["Length"]

final_w_supplierT.loc[final_w_supplierT["EJD_Weight"] == 0, "fail_width"] = np.nan
final_w_supplierF["Final_Width"] = final_w_supplierF["Width"]
# Quitando dimensiones a Skus que no son unidad base

final_w_supplierF[['EJD_Weight', 'EJD_Height', 'EJD_Length', 'EJD_Width']] = np.nan
final_w_supplier = pd.concat([final_w_supplierT, final_w_supplierF], ignore_index=True)
# SUMAR DIMENSIONES
df_cleanedT["Sum_Dim"] = df_cleanedT["Height"] + df_cleanedT["Length"] + df_cleanedT["Width"]
df_cleanedF["Sum_Dim"] = df_cleanedF["Height"] + df_cleanedF["Length"] + df_cleanedF["Width"]
df_cleanedT["Sum_Dim_EJD_Sup"] = df_cleanedT["th_height"] + df_cleanedT["th_length"] + df_cleanedT["th_width"]
df_cleanedT["Sum_Dim_EJD_Low"] = df_cleanedT["EJD_Height"] + df_cleanedT["EJD_Length"] + df_cleanedT["EJD_Width"]
# MEDIANA PESO Y DIMS
df_metricaT = df_cleanedT.groupby(["Sku", 'UomCode_N', 'BaseUnit'])[["Weight", "Sum_Dim", "EJD_Weight", "th_weight", "Sum_Dim_EJD_Low", "Sum_Dim_EJD_Sup"]].median().round(2).reset_index()
df_metricaF = df_cleanedF.groupby(["Sku", 'UomCode_N', 'BaseUnit'])[['Weight', "Sum_Dim"]].median().round(2).reset_index()
# FLAG PESO
i_w_low = df_metricaT[(df_metricaT["Weight"] < df_metricaT["EJD_Weight"])].index
i_w_high = df_metricaT[(df_metricaT["Weight"] > df_metricaT["th_weight"])].index

df_metricaT["Weight_Flag"] = "Within range"
df_metricaT.loc[i_w_low, "Weight_Flag"] = "Underweight"
df_metricaT.loc[i_w_high, "Weight_Flag"] = "Overweight"

df_metricaF["Weight_Flag"] = "No EJD Dims"
# FLAG DIMS
i_w_lowD = df_metricaT[(df_metricaT["Sum_Dim"] < df_metricaT["Sum_Dim_EJD_Low"])].index
i_w_highD = df_metricaT[(df_metricaT["Sum_Dim"] > df_metricaT["Sum_Dim_EJD_Sup"])].index

df_metricaT["DIM_Flag"] = "Within range"
df_metricaT.loc[i_w_lowD, "DIM_Flag"] = "Under"
df_metricaT.loc[i_w_highD, "DIM_Flag"] = "Over"

df_metricaF["DIM_Flag"] = "No EJD Dims"
df_cleaned = pd.concat([df_cleanedT, df_cleanedF], ignore_index=True)
df_metrica = pd.concat([df_metricaT, df_metricaF], ignore_index=True)
df_pred_evp = df_pred[['Sku', 'UomCode', 'UomQuantity', 'Weight', 'Height', 'Length', 'Width']]
from datetime import datetime
fecha = datetime.today().strftime('%Y%m%d')

# Suponiendo que tu DataFrame se llama df
chunk_size = 5000  # Tamaño de cada archivo
chunks = np.array_split(df_pred_evp, len(df_pred_evp) // chunk_size + (1 if len(df_pred_evp) % chunk_size != 0 else 0))  # Dividir en partes

# Ruta donde quieres guardar
ruta_base = r'C:\Users\ecalderon\OneDrive - BITS - Max Warehouse\Shipping\Weights Process'

# Guardar cada chunk en un CSV dentro de la ruta especificada
list(map(lambda x: x[1].to_csv(os.path.join(ruta_base, f"{fecha}_df_evp_{x[0]+1}.csv"), index=False), enumerate(chunks)))

print(f"Se han creado {len(chunks)} archivos CSV.")
from datetime import date

df_pred_evp['Fecha'] = date.today()
# Datos de conexión
servidor = '10.12.200.59'
base_de_datos = 'Maxwarehouse'

# Crear la conexión con autenticación de Windows
engine = create_engine(f"mssql+pyodbc://@{servidor}/{base_de_datos}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")

# Nombre de la tabla destino
table_name = 'DataPrediccionDims'

try:
    # Insertar datos en la tabla, sin duplicar registros, asegurando que se mantenga el índice único
    df_pred_evp.to_sql(table_name, con=engine, if_exists='append', index=False)
    print("✅ Registros insertados correctamente.")
except Exception as e:
    print(f"⚠️ Error al insertar los registros: {e}")
df_ventas = df_cleaned[['TranDate', 'InvDate', 'trackingnum','Sku', 'UomCode_N', 'UomQuantity', 'SalesStatusEVP','TotalSales', 'TotalCost']].drop_duplicates()
# crear campos de temporalidad

df_ventas["Year"] = df_ventas["TranDate"].dt.year
df_ventas["Month"] = df_ventas["TranDate"].dt.month_name()
df_ventas["YearMonth"] = df_ventas["TranDate"].dt.strftime("%Y-%m")
# Agrupar por Año-Mes y SKU, sumando las ventas
ventas_por_mes = df_ventas.groupby(["YearMonth", "Sku", "UomCode_N"])["TotalSales"].sum().reset_index()
ventas_por_mes = ventas_por_mes[ventas_por_mes['TotalSales'] != 0.00]
# Colocar los meses en columnas

df_transposed = ventas_por_mes.pivot_table(index=["Sku", 'UomCode_N'], columns="YearMonth", values="TotalSales", aggfunc="sum").round(2).reset_index()
# Calcular tendencia

# Columnas de meses
ventas_cols = df_transposed.columns[2:]

# Calcular la variación promedio mes a mes
df_transposed["Tendencia"] = df_transposed[ventas_cols].pct_change(axis=1).mean(axis=1, skipna=True).round(2)

# Reemplazar NaN por 0 si no hubo ventas en ningún mes
df_transposed = df_transposed.fillna(0)
df_metrica.isnull().sum()
df_metrica = df_metrica.merge(df_transposed, on=["Sku", "UomCode_N"], how="left")
ruta_base = r'C:\Users\ecalderon\OneDrive - BITS - Max Warehouse\Shipping\Weights Process'

# Guardar los otros DataFrames en la misma ruta
final_w_supplier.to_csv(os.path.join(ruta_base, "df_predicciones.csv"), index=False)
df_sorted.to_csv(os.path.join(ruta_base, "df_historico.csv"), index=False)
df_metrica.to_csv(os.path.join(ruta_base, "df_metrica.csv"), index=False)

server = '10.12.200.59' #ip del DB es 10.11.100.103
database = 'Maxwarehouse'

# Crear el engine usando la cadena de conexión para SQL Server con pyodbc
engine = create_engine(f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")

# Ejecutar consulta 

query2 = """

WITH 

endicia_semana_mes AS (
    SELECT DISTINCT  
        [Postmark_Date] AS TranDate, 
        [Transaction_Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
		[Transaction_Amount] as DiscountedNetCharge
    FROM Endiciatransacprovision
    WHERE ISDATE([Transaction_Date (GMT)]) = 1
      AND CAST([Transaction_Date (GMT)] AS date) >= DATEADD(MONTH, -4, GETDATE())
    
    UNION ALL
    
    SELECT DISTINCT  
        [Postmark Date] AS TranDate, 
        [Transaction Date (GMT)] AS InvDate, 
        REPLACE([Tracking Number], '''', '') AS trackingnum, 
		[Transaction Amount] as DiscountedNetCharge
    FROM endicia_semanal
    WHERE ISDATE([Transaction Date (GMT)]) = 1
      AND CAST([Transaction Date (GMT)] AS date) >= DATEADD(MONTH, -2, GETDATE())
),

Factura3meses AS (

    -- FEDEX
    SELECT DISTINCT
        CAST(InvoiceDate AS date) AS TranDate,
        CAST(InvoiceDate AS date) AS InvDate,
        GroundTrackingIDPrefix + ExpressorGroundTrackingID AS trackingnum,
        max(NetChargeAmount) as DiscountedNetCharge
    FROM 
        FEDEXHISTORICO
    GROUP BY 
        CAST(InvoiceDate AS date),
        CAST(InvoiceDate AS date),
        GroundTrackingIDPrefix + ExpressorGroundTrackingID

    UNION ALL
    -- UPS (CON PRIORIDAD PackageDimensions)
    SELECT 
    cast(TransactionDate as date), cast(InvoiceDate as date), TrackingNumber, sum(netamount) as DiscountedNetCharge
	FROM UPSNEWFORMAT
	GROUP BY 
	cast(TransactionDate as date), cast(InvoiceDate as date), TrackingNumber

    UNION ALL
    -- USPS (Endicia)
    SELECT DISTINCT 
        TranDate,
        InvDate,
        trackingnum,
        DiscountedNetCharge
    FROM endicia_semana_mes
),

DWVentasUnicas as (
	Select 
		SalesOrderNumber, Sku
	from DWShippingInfo as dws
	where cast(SalesOrderDate as date ) between dateadd(MONTH,-4, cast(GETDATE() as date)) and cast(GETDATE() as date)
	group by SalesOrderNumber, Sku
),

CantidadSkusxVenta as (
	select
		SalesOrderNumber, count(Sku) CantidadSkus
	from DWVentasUnicas dw 
	group by SalesOrderNumber
	having count(Sku) =1
),

PesosfacturaVentas as (
	select  
		TranDate, InvDate, dw.trackingnum, mw.FulfillmentOrderNumber, dw.DiscountedNetCharge, 
		mw.Sku, mw.UomCode, mw.UomQuantity, mw.EstimatedShippingCost
	FROM Factura3meses dw
	INNER JOIN DWShippingInfo  mw
		ON dw.trackingnum = mw.trackingnum
	WHERE dw.trackingnum !=''
	and SalesOrderNumber in (select SalesOrderNumber from CantidadSkusxVenta)
	and cast(UomQuantity as float)/cast(Quantity as float)= 1
)

select InvDate,trackingnum, FulfillmentOrderNumber, sum(DiscountedNetCharge) as DiscountedNetCharge, Sku, UomCode, UomQuantity, 
max(EstimatedShippingCost) as EstimatedCost  
from PesosfacturaVentas
group by InvDate,trackingnum, FulfillmentOrderNumber, Sku, UomCode, UomQuantity
having InvDate >= DATEADD(WEEK, -2, GETDATE())

"""

campos_salida = pd.read_sql(query2, con=engine)
campos_salida = normalize_uom_code(campos_salida)
base_salida = final_w_supplier[["Sku", "UomCode", "Final_Height", "Final_Length", "Final_Width", "Final_Weight"]]
salida_evp = pd.merge(campos_salida, base_salida, on=["Sku", "UomCode"], how="left")
salida_evp = salida_evp[['trackingnum', 'FulfillmentOrderNumber', "Final_Height", "Final_Length", "Final_Width", "Final_Weight", 'DiscountedNetCharge']]
salida_evp.rename(columns={"trackingnum": "TrackingNumber", 
                           "FulfillmentOrderNumber": "ShipperReference",
                           "Final_Height": "PackageHeight",
                           "Final_Length": "PackageLength",
                           "Final_Width": "PackageWidth",
                           "Final_Weight": "BilledWeight"}, inplace=True)
#salida_evp.to_csv("df_evp.csv", index=False)
campos_salida.rename(columns={'DiscountedNetCharge': 'Cost'}, inplace=True)
# convertir tipo de dato
campos_salida['Cost'] = pd.to_numeric(campos_salida['Cost'], errors='coerce')
campos_salida['EstimatedCost'] = pd.to_numeric(campos_salida['EstimatedCost'], errors='coerce')
# Variacion porcentual del costo
campos_salida['variacion_costo'] = ((campos_salida['Cost'] - campos_salida['EstimatedCost']) / campos_salida['EstimatedCost']).round(2)
# Semana del reporte
campos_salida['Semana'] = campos_salida['InvDate'].dt.isocalendar().week
campos_salida[(campos_salida['variacion_costo'] < 0) & (campos_salida['EstimatedCost'] != 0)].sort_values('variacion_costo', ascending=True)
# Ruta completa al archivo costos.csv
ruta_costos = os.path.join(ruta_base, 'costos.csv')

# Guardar en la ruta especificada (modo append)
campos_salida.to_csv(ruta_costos, mode='a', header=False, index=False)
server = '10.12.200.59' #ip del DB es 10.11.100.103
database = 'Maxwarehouse'

# Crear el engine usando la cadena de conexión para SQL Server con pyodbc
engine = create_engine(f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server")

# Ejecutar consulta 

query = """
select * from [stg-MaxWarehouse].[EvpCsv].[EVPAlllSKUsDimNWeight]
"""

df_evp = pd.read_sql(query, con=engine)
df_evp = df_evp[['EVP SKU', 'EVP UOM CODE', 'EVP UOM QTY', 'EVP Weight', 'EVP Height', 'EVP Length', 'EVP Width']]
df_merge = df_pred.merge(
    df_evp,  
    left_on=['Sku', 'UomCode'], 
    right_on=['EVP SKU', 'EVP UOM CODE'], 
    how='left'
)
# EVP solo toma sku base unit
#df_bu = df_merge[df_merge['BaseUnit'] == True]

# Quitando nulos
df_bu = df_merge[~df_merge['EVP Width'].isnull()]
# convertir tipo de dato
df_bu['EVP Weight'] = pd.to_numeric(df_bu['EVP Weight'], errors='coerce')
df_bu['EVP Height'] = pd.to_numeric(df_bu['EVP Height'], errors='coerce')
df_bu['EVP Length'] = pd.to_numeric(df_bu['EVP Length'], errors='coerce')
df_bu['EVP Width'] = pd.to_numeric(df_bu['EVP Width'], errors='coerce')
# Fecha del reporte
df_bu['Fecha'] = pd.to_datetime('today').normalize()
df_bu['Semana'] = df_bu['Fecha'].dt.isocalendar().week
# Suma dimensiones
df_bu['Sum_Dims_Pred'] = df_bu['Height'] + df_bu['Length'] + df_bu['Width']
df_bu['Sum_Dims_EVP'] = df_bu['EVP Height'] + df_bu['EVP Length'] + df_bu['EVP Width']
# Variacion porcentual
df_bu['variacion_weight'] = ((df_bu['Weight'] - df_bu['EVP Weight']) / df_bu['EVP Weight']).round(2)
df_bu['variacion_dims'] = ((df_bu['Sum_Dims_Pred'] - df_bu['Sum_Dims_EVP']) / df_bu['Sum_Dims_EVP']).round(2)
df_bu = df_bu.rename(columns={'EVP Weight': 'EVP_Weight', 'EVP Height': 'EVP_Height', 'EVP Length': 'EVP_Length', 'EVP Width': 'EVP_Width'})
# Ruta completa al archivo
ruta_cuantificacion = os.path.join(ruta_base, 'cuantificacion_skus.csv')

# Guardar en la ruta especificada (modo append)
df_bu.to_csv(ruta_cuantificacion, mode='a', header=False, index=False)
def send_email_final():
    # Configuración del correo
    sender_email = "ecalderon@maxwarehouse.com"  
    sender_password = "Maxwarehouse2025$"  

    # 🔹 Lista de destinatarios
    recipient_list = ["ecalderon@maxwarehouse.com", "vpverbena@maxwarehouse.com"]
    
    # Contenido del correo
    subject = "🔔 Alerta: Generación de predicciones de pesos completada"
    body = "El modelo de predicción ha finalizado correctamente. Sube los resultados a EVP."

    # Crear el mensaje
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_list)

    try:
        # Conectar al servidor SMTP (ejemplo para Gmail)
        server = smtplib.SMTP("smtp.office365.com", 587)  # Usa "smtp.office365.com" para Outlook
        server.starttls()  # Seguridad TLS
        server.login(sender_email, sender_password)  # Autenticación
        server.sendmail(sender_email, recipient_list, msg.as_string())  # Enviar
        server.quit()
        print("✅ Correo enviado correctamente")
    except Exception as e:
        print(f"⚠ Error al enviar el correo: {e}")

# Llamar a la función al final del proceso
send_email_final()

