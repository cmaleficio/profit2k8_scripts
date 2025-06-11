## Conexion a la API de pydolarve
import os
import schedule
import time
import requests
import pyodbc
import datetime
import tlg_bot_tasa
from decimal import Decimal
from dotenv import load_dotenv, dotenv_values
# Configuración

load_dotenv()
SERVER = os.getenv("SERVER")
DATABASE=  os.getenv("DATABASE_PROD")
USERNAME = os.getenv("USER")
PASSWORD = os.getenv("PASS")


def fetch_and_insert_dollar_rate():
    try:
        # Establecer conexión a la base de datos
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD};'
            'Encrypt=no;'
        )
        cursor = conn.cursor()

        # Conexión a la API de pydolarve
        response = requests.get(
            "https://pydolarve.org/api/v2/tipo-cambio",
            params={
                "currency": "usd",
                "format_date": "default",
                "rounded_price": "false",
            }
        )

        tasas_query = """
        INSERT INTO tasas (co_mone, fecha, tasa_c, tasa_v, co_us_in, fe_us_in, co_us_mo, fe_us_mo, co_us_el, fe_us_el, trasnfe, revisado, co_sucu, rowguid)
        VALUES ('USD', CONVERT(date, GETDATE()), 0, ?, '850', GETDATE(), ' ', GETDATE(), ' ', GETDATE(), ' ', ' ', 'UNI', NEWID())
        """

        moneda_query = """ 
        update moneda set cambio = ? where co_mone = 'USD'
        """

        # Verificar si la respuesta fue exitosa y obtener el valor del dólar
        if response.status_code == 200:
            print("Conexión exitosa a la API de pydolarve")
            usd = response.json().get("price")
            print(f"El valor del dólar es: {usd}")
            # Insertar el valor del dólar en la base de datos
            try:
                cursor.execute(tasas_query, usd)
                cursor.execute(moneda_query, usd)
                conn.commit()
                print("Valor del dólar insertado en la tabla tasas y actualizado en la tabla moneda")
                # Enviar mensaje al bot de Telegram
                tlg_bot_tasa.bot_send_text(f"Se ha actualizado la Tasa del dia en el sistema a : {usd}")
            except pyodbc.Error as e:
                print(f"Error al insertar el valor del dólar en la base de datos: {str(e)}")
                tlg_bot_tasa.bot_send_text(f"Error al insertar el valor del dólar en la base de datos: {str(e)}")
        else:
            print(f"Error en la conexión a la API de pydolarve: {response.status_code}")
            tlg_bot_tasa.bot_send_text(f"Hubo problemas al intentar conectar a la API de pydolarve: {response.status_code}")

        # Cerrar conexión
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")

def is_weekday():
    # Check if today is Monday through Friday (0 = Monday, 6 = Sunday)
    today = datetime.datetime.now().weekday()
    return today < 6  # Returns True for Monday (0) to Friday (4)

def job():
    # Only run the fetch_and_insert_dollar_rate function on weekdays
    if is_weekday():
        print(f"Ejecutando tarea a las {datetime.datetime.now()}")
        fetch_and_insert_dollar_rate()
    else:
        print("Hoy es domingo, no se ejecuta la tarea.")

# Programar la tarea para que se ejecute a las 00:02 de lunes a viernes
#schedule.every(1).minute.at("00:01").do(job)
schedule.every().day.at("00:00").do(job)

# Bucle principal para mantener el script corriendo
if __name__ == "__main__":
    print("Script iniciado. Esperando la hora programada (12:00 AM, Lunes a Sabado)...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Esperar 60 segundos antes de verificar nuevamente

