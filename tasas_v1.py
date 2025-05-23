## Conexion a la API de pydolarve
import os
import requests
import pyodbc
from decimal import Decimal
import tlg_bot_tasa
from dotenv import load_dotenv, dotenv_values

# Configuración

load_dotenv()
SERVER = os.getenv("SERVER")
DATABASE=  os.getenv("DATABASE_PRUEBA")
USERNAME = os.getenv("USER")
PASSWORD = os.getenv("PASS")

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD};'
    'Encrypt=no;'
)
cursor = conn.cursor()

response = requests.get("https://pydolarve.org/api/v2/tipo-cambio",
    params={
      "currency": "usd",
      "format_date": "default",
      "rounded_price": "false",
    }
)

tasas_query = """ 
INSERT INTO tasas (co_mone, fecha, tasa_c, tasa_v, co_us_in, fe_us_in, co_us_mo, fe_us_mo, co_us_el, fe_us_el, trasnfe, revisado, co_sucu, rowguid)
VALUES ('USD',CONVERT(date,GETDATE()), 0, ?, '850', GETDATE(), ' ', GETDATE(), ' ', GETDATE(), ' ', ' ', 'UNI', NEWID())
"""
# Verificar si la respuesta fue exitosa y obtener el valor del dólar
if response.status_code == 200:
    print("Conexión exitosa a la API de pydolarve")
    #print(response.json())
    usd = response.json().get("price")
    print (f"El valor del dolar es: {usd}")
    # Insertar el valor del dólar en la base de datos
    try:
        cursor.execute(tasas_query,usd)
        conn.commit()
        print("Valor del dólar insertado en la base de datos")
        tlg_bot_tasa.bot_send_text(f"Se ha actualizado la Tasa del dia en el sistema a : {usd}") 
    except pyodbc.Error as e:
        print(f"Error al insertar el valor del dólar en la base de datos: {str(e)}")
        tlg_bot_tasa.bot_send_text(f"Error al insertar el valor del dólar en la base de datos: {str(e)}")
else:
    print("Error en la conexión a la API de pydolarve")
    tlg_bot_tasa.bot_send_text(f"Error en la conexión a la API de pydolarve: {response.status_code}")
