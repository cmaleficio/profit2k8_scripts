import os
import schedule
import time
import requests
import pyodbc
import datetime
import tlg_bot_tasa
import logging
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    filename='C:/Users/cmarffisis/Desktop/PROFIT/TASAS/tasas_v2.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def load_env_and_validate():
    """Carga las variables de entorno y valida que existan todas las necesarias."""
    load_dotenv()
    SERVER = os.getenv("SERVER")
    DATABASE = os.getenv("DATABASE_PROD")
    USERNAME = os.getenv("USER")
    PASSWORD = os.getenv("PASS")
    required_vars = {
        "SERVER": SERVER,
        "DATABASE_PROD": DATABASE,
        "USER": USERNAME,
        "PASS": PASSWORD
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        logging.error(f"Faltan variables de entorno: {', '.join(missing)}")
        raise ValueError(f"Faltan variables de entorno: {', '.join(missing)}")
    return SERVER, DATABASE, USERNAME, PASSWORD

def fetch_and_insert_dollar_rate():
    """Consulta la tasa del dólar y la inserta/actualiza en la base de datos."""
    try:
        SERVER, DATABASE, USERNAME, PASSWORD = load_env_and_validate()
        # Establecer conexión a la base de datos usando context manager
        with pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD};'
            'Encrypt=no;'
        ) as conn:
            with conn.cursor() as cursor:
                logging.info("Conexión exitosa a la base de datos")

                # Conexión a la API de pydolarve con timeout y manejo de errores
                try:
                    response = requests.get(
                        "https://pydolarve.org/api/v2/tipo-cambio",
                        params={
                            "currency": "usd",
                            "format_date": "default",
                            "rounded_price": "false",
                        },
                        timeout=10
                    )
                except requests.RequestException as e:
                    logging.error(f"Error de red al consultar la API: {e}")
                    tlg_bot_tasa.bot_send_text(f"Error de red al consultar la API: {e}")
                    return

                tasas_query = """
                INSERT INTO tasas (co_mone, fecha, tasa_c, tasa_v, co_us_in, fe_us_in, co_us_mo, fe_us_mo, co_us_el, fe_us_el, trasnfe, revisado, co_sucu, rowguid)
                VALUES ('USD', CONVERT(date, GETDATE()), 0, ?, '850', GETDATE(), ' ', GETDATE(), ' ', GETDATE(), ' ', ' ', 'UNI', NEWID())
                """

                moneda_query = """ 
                UPDATE moneda SET cambio = ? WHERE co_mone = 'USD'
                """

                # Verificar si la respuesta fue exitosa y obtener el valor del dólar
                if response.status_code == 200:
                    logging.info("Conexión exitosa a la API de pydolarve")
                    usd = response.json().get("price")
                    try:
                        usd = float(usd)
                    except (TypeError, ValueError):
                        logging.error(f"Valor de dólar inválido recibido: {usd}")
                        tlg_bot_tasa.bot_send_text(f"Valor de dólar inválido recibido: {usd}")
                        return

                    try:
                        cursor.execute(tasas_query, usd)
                        cursor.execute(moneda_query, usd)
                        conn.commit()
                        logging.info("Valor del dólar insertado en la tabla tasas y actualizado en la tabla moneda")
                        tlg_bot_tasa.bot_send_text(f"Se ha actualizado la Tasa del día en el sistema a: {usd}")
                    except pyodbc.Error as e:
                        logging.error(f"Error al insertar el valor del dólar en la base de datos: {str(e)}")
                        tlg_bot_tasa.bot_send_text(f"Error al insertar el valor del dólar en la base de datos: {str(e)}")
                else:
                    logging.error(f"Error en la conexión a la API de pydolarve: {response.status_code}")
                    tlg_bot_tasa.bot_send_text(f"Hubo problemas al intentar conectar a la API de pydolarve: {response.status_code}")

    except Exception as e:
        logging.error(f"Error durante la ejecución: {str(e)}")
        tlg_bot_tasa.bot_send_text(f"Hubo otro tipo de problemas: {str(e)}")

def is_weekday():
    """Devuelve True si hoy es lunes a viernes."""
    today = datetime.datetime.now().weekday()
    return today < 6  # 0 = lunes, 5 = sabado

def job():
    """Ejecuta la actualización solo si es día hábil."""
    if is_weekday():
        logging.info(f"Ejecutando tarea a las {datetime.datetime.now()}")
        fetch_and_insert_dollar_rate()
    else:
        logging.info("Hoy es sábado o domingo, no se ejecuta la tarea.")

# Programar la tarea para que se ejecute a las 00:00 de lunes a viernes
schedule.every().day.at("00:00").do(job)

if __name__ == "__main__":
    logging.info("Script iniciado. Esperando la hora programada (12:00 AM, Lunes a Sabado)...")
    while True:
        schedule.run_pending()
        time.sleep(60)