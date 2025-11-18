import csv
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# ==========================================================
# CONFIGURACIÓN
# ==========================================================

# Archivo CSV local
CSV_FILENAME = "smart_home_historico.csv"

# Nombre del archivo de credenciales JSON de Google
CREDENTIALS_FILE = "credentials.json"

# Nombre de la hoja de cálculo en Google Sheets
SPREADSHEET_NAME = "Smart Home Data"

# Alcances necesarios para Google Sheets
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# ==========================================================
# FUNCIONES
# ==========================================================

def conectar_google_sheets():
    """
    Conecta con Google Sheets usando las credenciales de servicio
    """
    print("Conectando con Google Sheets...")
    
    # Cargar credenciales
    credentials = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=SCOPES
    )
    
    # Autorizar cliente
    client = gspread.authorize(credentials)
    
    print("✓ Conectado exitosamente")
    return client

def obtener_o_crear_spreadsheet(client, nombre):
    """
    Obtiene una hoja de cálculo existente o crea una nueva
    """
    try:
        # Intentar abrir hoja existente
        spreadsheet = client.open(nombre)
        print(f"✓ Hoja '{nombre}' encontrada")
    except gspread.SpreadsheetNotFound:
        # Crear nueva hoja
        spreadsheet = client.create(nombre)
        print(f"✓ Nueva hoja '{nombre}' creada")
    
    return spreadsheet

def leer_csv(filename):
    """
    Lee el archivo CSV y retorna los datos
    """
    print(f"Leyendo datos de {filename}...")
    
    datos = []
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            datos.append(row)
    
    print(f"✓ {len(datos)} filas leídas")
    return datos

def subir_datos_completos(worksheet, datos):
    """
    Sube todos los datos del CSV a la hoja
    Borra el contenido anterior
    """
    print("Subiendo todos los datos...")
    
    # Limpiar hoja
    worksheet.clear()
    
    # Subir datos
    worksheet.update('A1', datos)
    
    print(f"✓ {len(datos)} filas subidas")

def subir_datos_incrementales(worksheet, datos):
    """
    Solo agrega las filas nuevas que no existen en la hoja
    """
    print("Subiendo datos incrementales...")
    
    # Obtener datos existentes
    datos_existentes = worksheet.get_all_values()
    
    if not datos_existentes:
        # Si la hoja está vacía, subir todo
        worksheet.update('A1', datos)
        print(f"✓ {len(datos)} filas subidas (hoja estaba vacía)")
        return
    
    # Obtener la última fila de la hoja
    ultima_fila = len(datos_existentes)
    
    # Si el CSV tiene más datos que la hoja
    if len(datos) > ultima_fila:
        # Obtener solo las filas nuevas
        filas_nuevas = datos[ultima_fila:]
        
        # Agregar al final de la hoja
        worksheet.append_rows(filas_nuevas)
        print(f"✓ {len(filas_nuevas)} filas nuevas agregadas")
    else:
        print("✓ No hay filas nuevas para agregar")

def formatear_hoja(worksheet):
    """
    Aplica formato básico a la hoja
    """
    print("Aplicando formato...")
    
    # Formato del encabezado (primera fila)
    worksheet.format('A1:F1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9}
    })
    
    # Ajustar ancho de columnas
    worksheet.set_column_width('A', 200)  # datetime
    worksheet.set_column_width('B', 100)  # sound_avg
    worksheet.set_column_width('C', 80)   # motion
    worksheet.set_column_width('D', 80)   # temp
    worksheet.set_column_width('E', 80)   # hum
    worksheet.set_column_width('F', 80)   # dist
    
    print("✓ Formato aplicado")

# ==========================================================
# MAIN
# ==========================================================

def main():
    print("\n=== Subir CSV a Google Sheets ===\n")
    
    try:
        # 1. Conectar con Google Sheets
        client = conectar_google_sheets()
        
        # 2. Obtener o crear la hoja de cálculo
        spreadsheet = obtener_o_crear_spreadsheet(client, SPREADSHEET_NAME)
        
        # 3. Obtener la primera worksheet (pestaña)
        worksheet = spreadsheet.sheet1
        
        # 4. Leer datos del CSV
        datos = leer_csv(CSV_FILENAME)
        
        # 5. Subir datos (elegir uno de los dos métodos)
        
        # OPCIÓN A: Subir todo (borra y reemplaza)
        # subir_datos_completos(worksheet, datos)
        
        # OPCIÓN B: Solo agregar filas nuevas (recomendado)
        subir_datos_incrementales(worksheet, datos)
        
        # 6. Formatear hoja
        formatear_hoja(worksheet)
        
        # 7. Obtener URL de la hoja
        print(f"\n✓ Proceso completado!")
        print(f"URL: {spreadsheet.url}")
        
    except FileNotFoundError:
        print(f"\n✗ Error: No se encontró el archivo '{CSV_FILENAME}'")
        print("Asegúrate de que el archivo CSV existe en el mismo directorio")
        
    except FileNotFoundError as e:
        if 'credentials.json' in str(e):
            print(f"\n✗ Error: No se encontró '{CREDENTIALS_FILE}'")
            print("\nPasos para obtener las credenciales:")
            print("1. Ve a: https://console.cloud.google.com/")
            print("2. Crea un proyecto nuevo o selecciona uno existente")
            print("3. Habilita la API de Google Sheets y Google Drive")
            print("4. Ve a 'Credenciales' y crea una 'Cuenta de servicio'")
            print("5. Descarga el archivo JSON de credenciales")
            print("6. Renómbralo como 'credentials.json' y colócalo en este directorio")
        else:
            raise
            
    except Exception as e:
        print(f"\n✗ Error inesperado: {e}")

if __name__ == "__main__":
    main()