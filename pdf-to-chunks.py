import os, time, uuid, json, subprocess, shutil, platform
import pytesseract
from pdf2image import convert_from_path
from pathlib import Path
from azure.storage.blob import BlobClient
from dotenv import load_dotenv
import json
from db_conector import db_conection
from log import get_logger

OUT_PATH  = "./descargas/file.pdf" # ruta local de salida
CONTAINER_PDF = "v-ia-files"
CONTAINER_CHUNKS = "v-ia-chunks"
CHUNK_SIZE = 7000
CHUNK_OVERLAP = 600

def createBlobClient(blobName, containerName):
    # Se carga .env si existe
    load_dotenv()
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
            raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING (o pásala como conn_str).")
    print(conn)
    # Crear el cliente del blob
    return BlobClient.from_connection_string(conn_str=conn, container_name= containerName, blob_name=blobName)

def getPdfFromBlob(pdfName, outPath=OUT_PATH):
    # nombre del archivo pdf a descargar
    blobName = f"{pdfName}.pdf"

    # Crear el cliente del blob
    blob = createBlobClient(blobName, CONTAINER_PDF)


    Path(outPath).parent.mkdir(parents=True, exist_ok=True)
    print(blobName)
    with open(outPath, "wb") as f:
        f.write(blob.download_blob().readall())


def chunk_text(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    text =   text.strip()
    print(f"Texto total a chunkear: {len(text)} caracteres.")
    if not text:
        return []
    chunks, start = [], 0
    while start < len(text):
        end = min(start + size, len(text))
        cut = max(text.rfind("\n\n", start, end), text.rfind(". ", start, end))
        print(f"Chunk desde {start} hasta {end}, corte en {cut}.")
        if cut == -1 or cut < start + int(size * 0.5):
            cut = end
        chunks.append(text[start:cut].strip())
        if cut == len(text):
            break
        start = max(0, cut - overlap)
    return chunks

def ensure_tesseract():
    # Si el usuario puso una ruta manual, respétala; si no, detecta
    if shutil.which("tesseract"):
        return
    if platform.system() == "Windows":
        win_path = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        if os.path.exists(win_path):
            pytesseract.pytesseract.tesseract_cmd = win_path
            return
    raise RuntimeError(
        "Tesseract no encontrado. Instálalo (Ubuntu: 'sudo apt install tesseract-ocr tesseract-ocr-spa')."
    )

def extract_text_with_pdftotext(pdf_path):
    """Devuelve el texto usando pdftotext si está disponible; '' si no hay texto o no está."""
    if not shutil.which("pdftotext"):
        return ""
    try:
        out = subprocess.check_output(
            ["pdftotext", "-layout", pdf_path, "-"],
            stderr=subprocess.STDOUT
        )
        txt = out.decode("utf-8", "ignore").strip()
        return txt
    except subprocess.CalledProcessError:
        return ""

def ocr_pdf_to_chunks(pdfPath, pdfName, lang="spa"):
    
    print(f"📄 Procesando PDF: {pdfPath}")
    doc_id = os.path.basename(pdfPath)

    # 1) Intentar texto directo (más rápido/preciso si no es escaneado)
    # direct_txt = extract_text_with_pdftotext(pdfPath)
    all_chunks = []

    # if direct_txt:
    #     for j, part in enumerate(chunk_text(direct_txt)):
    #         all_chunks.append({
    #             "id": str(uuid.uuid4()),
    #             "doc_id": doc_id,
    #             "page": None,
    #             "chunk_idx": j + 1,
    #             "content": part
    #         })
    # else:
        # 2) Si no hay texto, hacer OCR
    ensure_tesseract()
    pages = convert_from_path(pdfPath, dpi=300)  # requiere poppler-utils
    for i, img in enumerate(pages, start=1):
        print(f"🖼️ Procesando página {i}/{len(pages)}...")
        txt = pytesseract.image_to_string(img, lang=lang)
        for j, part in enumerate(chunk_text(txt)):
            all_chunks.append({
                "id": str(uuid.uuid4()),
                "doc_id": f"{pdfName}.pdf",
                "page": i,
                "chunk_idx": j + 1,
                "content": part
            })
        print(f"📝 Página {i}/{len(pages)} procesada, {len(all_chunks)} chunks hasta ahora.")

    # Guardar chunks en Azure Blob Storage
    blobName = f"{pdfName}.jsonl"

    # Crear el cliente del blob
    blob = createBlobClient(blobName, CONTAINER_CHUNKS)

    # blob.upload_blob(all_chunks.encode("utf-8"), overwrite=True)
    jsonl_str = "\n".join(json.dumps(x, ensure_ascii=False) for x in all_chunks)
    blob.upload_blob(jsonl_str.encode("utf-8"), overwrite=True)

    print(f"✅ {len(all_chunks)} chunks guardados del archivo {pdfName}")
    

if __name__ == "__main__":
    # inicializar logs
    log = get_logger("chunks")
    log.info("Iniciando OCR de PDF")
    total_time = 0

    for file_id in range(1224,1622):
        start_t = time.time()
        try:
            #ingresamos el nombre del pdf
            # pdf_name = input("Ingresa el nombre del PDF (sin extensión): ").strip()
            pdf_name = db_conection(file_id)
            pdf_name, _ = os.path.splitext(pdf_name)

            # Obtenemos PDF de azure blob storage
            getPdfFromBlob(pdf_name)
            print(f"✅ PDF {pdf_name} descargado exitosamente.")

            if not os.path.exists(OUT_PATH):
                raise FileNotFoundError(f"No se encontró {OUT_PATH} en el directorio actual.")
            ocr_pdf_to_chunks(OUT_PATH, pdf_name)
            log.info(f"PDF {pdf_name} con file_id {file_id} procesado exitosamente en {time.time() - start_t:.2f} s.")
            print(f"✅ Proceso completado para {pdf_name}.")
        except Exception as e:
            log.error(f"Error procesando PDF {pdf_name}: {e}")
            print(f"❌ Ocurrió un error: {e}")
        print(f"⏱️ Tiempo total: {time.time() - start_t:.2f} s")
        total_time += time.time() - start_t
        print(f"⏳ Tiempo acumulado para todos los PDFs: {total_time:.2f} s")

    log.info(f"Tiempo acumulado para todos los PDFs: {total_time:.2f} s")