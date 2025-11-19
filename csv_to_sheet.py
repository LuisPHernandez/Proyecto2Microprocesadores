import csv
import gspread
from google.oauth2.service_account import Credentials

# ==========================================================
# CONFIGURACIÓN-- python csv_to_sheet.py 
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
    
    print(" Conectado exitosamente")
    return client

def obtener_spreadsheet(client, nombre):
    """
    Obtiene una hoja de cálculo existente
    """
    try:
        spreadsheet = client.open(nombre)
        print(f" Hoja '{nombre}' encontrada")
        return spreadsheet
    except gspread.SpreadsheetNotFound:
        print(f" Hoja '{nombre}' no encontrada")
        print("\n INSTRUCCIONES:")
        print("1. Crea manualmente una hoja en Google Sheets llamada 'Smart Home Data'")
        print("2. Compártela con el email de tu cuenta de servicio")
        print("   (Abre credentials.json y busca 'client_email')")
        print("3. Darle permisos de 'Editor'")
        print("4. Vuelve a ejecutar este script")
        raise
    except Exception as e:
        print(f" Error: {e}")
        raise

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
    
    print(f" {len(datos)} filas leídas")
    return datos

def subir_datos_incrementales(worksheet, datos):
    """
    Solo agrega las filas nuevas que no existen en la hoja
    """
    print("Subiendo datos incrementales...")
    
    # Obtener datos existentes
    datos_existentes = worksheet.get_all_values()
    
    if not datos_existentes:
        # Si la hoja está vacía, subir todo
        worksheet.update('A1', datos, value_input_option='RAW')
        print(f" {len(datos)} filas subidas (hoja estaba vacía)")
        return
    
    # Obtener la última fila de la hoja
    ultima_fila = len(datos_existentes)
    
    # Si el CSV tiene más datos que la hoja
    if len(datos) > ultima_fila:
        # Obtener solo las filas nuevas
        filas_nuevas = datos[ultima_fila:]
        
        # Agregar al final de la hoja
        worksheet.append_rows(filas_nuevas, value_input_option='RAW')
        print(f"{len(filas_nuevas)} filas nuevas agregadas")
    else:
        print("No hay filas nuevas para agregar")

def formatear_hoja(worksheet):
    """
    Aplica formato básico a la hoja
    """
    print("Aplicando formato...")
    
    try:
        # Formato del encabezado (primera fila)
        worksheet.format('A1:F1', {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
            'horizontalAlignment': 'CENTER'
        })
        
        # Intentar ajustar ancho de columnas (compatible con múltiples versiones)
        try:
            # Método nuevo
            worksheet.columns_auto_resize(0, 5)
        except:
            # Si falla, no es crítico
            pass
        
        print(" Formato aplicado")
        
    except Exception as e:
        print(f" Advertencia al aplicar formato: {e}")
        print("  (Los datos se subieron correctamente)")

# ==========================================================
# MAIN
# ==========================================================

def main():
    print("\n" + "="*50)
    print("     SUBIR CSV A GOOGLE SHEETS")
    print("="*50 + "\n")
    
    try:
        # 1. Conectar con Google Sheets
        client = conectar_google_sheets()
        
        # 2. Obtener la hoja de cálculo
        spreadsheet = obtener_spreadsheet(client, SPREADSHEET_NAME)
        
        # 3. Obtener la primera worksheet (pestaña)
        worksheet = spreadsheet.sheet1
        
        # 4. Leer datos del CSV
        datos = leer_csv(CSV_FILENAME)
        
        # 5. Subir datos (modo incremental)
        print("\n Modo: INCREMENTAL (solo agrega filas nuevas)")
        subir_datos_incrementales(worksheet, datos)
        
        # 6. Formatear hoja
        formatear_hoja(worksheet)
        
        # 7. Mostrar resultado
        print("\n" + "="*50)
        print("PROCESO COMPLETADO EXITOSAMENTE")
        print("="*50)
        print(f"\n URL: {spreadsheet.url}\n")
        
    except FileNotFoundError as e:
        if CSV_FILENAME in str(e):
            print(f"\n Error: No se encontró el archivo '{CSV_FILENAME}'")
            print("   hay que ver que  el archivo CSV existe en el mismo directorio")
        elif CREDENTIALS_FILE in str(e):
            print(f"\n Error: No se encontró '{CREDENTIALS_FILE}'")
            print("\n PASOS PARA OBTENER CREDENCIALES:")
            print("1. Ve a: https://console.cloud.google.com/")
            print("2. Crea un proyecto o selecciona uno existente")
            print("3. Habilita Google Sheets API y Google Drive API")
            print("4. Ve a 'Credenciales' → 'Crear credenciales' → 'Cuenta de servicio'")
            print("5. Descarga el archivo JSON")
            print("6. Renómbralo como 'credentials.json' y colocarlo aquí")
        else:
            print(f"\nError de archivo: {e}")
            
    except gspread.SpreadsheetNotFound:
        # Ya se maneja en obtener_spreadsheet()
        pass
        
    except Exception as e:
        print(f"\n Error inesperado: {e}")
        print("\n Verifica que:")
        print("   - se Compartiera la hoja con la cuenta de servicio")
        print("   - darle permisos de 'Editor'")
        print("   - El nombre de la hoja es exactamente 'Smart Home Data'")

if __name__ == "__main__":
    main()