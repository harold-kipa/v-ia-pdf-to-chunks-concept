import pytesseract
import time
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
import PyPDF2

from pdf2image import convert_from_path
import os
import re
from datetime import datetime
import pyodbc
from dotenv import load_dotenv

def listRoutes(ruta_base):
        rutas = []
        for root, dirs, files in os.walk(ruta_base):
            for archivo in files:
                ruta_completa = os.path.join(root, archivo)
                rutas.append(ruta_completa)
        return rutas
load_dotenv()
USER = os.getenv("SQLSERVER_USER")
PASS = os.getenv("SQLSERVER_PASS")
HOST = os.getenv("SQLSERVER_HOST")
DB   = os.getenv("SQLSERVER_DB")
SCHEMA = os.getenv("SQLSERVER_SCHEMA")




conectionText = 'DRIVER={ODBC Driver 17 for SQL Server};'+ f'SERVER={HOST};DATABASE={DB};UID={USER};PWD={PASS};Encrypt=yes;TrustServerCertificate=yes;'

print(conectionText)


inicio = time.time()

pdf_path = r"occired-payments\2023"
listas = listRoutes(pdf_path)
print(f"Rutas encontradas: {len(listas)}")
input("Presiona Enter para iniciar el procesamiento de los archivos PDF...")

for i in range(2218, len(listas)):
    ruta = listas[i]
    conn = pyodbc.connect(conectionText)
    cursor = conn.cursor()
    print(f"Procesando: {listas[i]}")
    print(f"indice: {i}")
    with open(listas[i], "rb") as archivo:
        
        lector = PyPDF2.PdfReader(archivo)
        texto = lector.pages[0].extract_text()
        
        print(f"\n----- Página {1} -----\n")
        print(texto)
        patron = r"IVA No\. Transacción0\s+([A-Z0-9]+)"

        resultado = re.search(patron, texto)

        if resultado:
            consecutive = resultado.group(1)
            print("Número extraído:", consecutive)
            file_name = os.path.basename(ruta)
            file_path = ruta
            path_fixed = ruta.replace("\\", "/")
            file_url = f"https://proanalitica.blob.core.windows.net/v-ia-files/{path_fixed}"
            file_type = os.path.splitext(file_name)[1].replace(".", "")
            creation_date = datetime.now()
            print("""
                INSERT INTO master_kipa.tbl_files_occired_temp (consecutive, file_name, file_path, file_url, file_type, creation_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, consecutive, file_name, file_path, file_url, file_type, creation_date)
            cursor.execute("""
                INSERT INTO master_kipa.tbl_files_occired_temp (consecutive, file_name, file_path, file_url, file_type, creation_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, consecutive, file_name, file_path, file_url, file_type, creation_date)


            conn.commit()
            cursor.close()
            conn.close()
        else:
            print("No se encontró el número")
            # input("Presiona Enter para continuar con la siguiente página...")