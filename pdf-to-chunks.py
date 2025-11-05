import os, time, uuid, json, subprocess, shutil, platform
import pytesseract
from pdf2image import convert_from_path
from pathlib import Path
from azure.storage.blob import BlobClient
from dotenv import load_dotenv
import json
from log import get_logger

OUT_PATH  = "./descargas/file.pdf" # ruta local de salida
CONTAINER_PDF = "v-ia-files"
CONTAINER_CHUNKS = "v-ia"
CHUNK_SIZE = 300
CHUNK_OVERLAP = 50

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
    blobName = f"contratos/{pdfName}.pdf"

    # Crear el cliente del blob
    blob = createBlobClient(blobName, CONTAINER_PDF)

    Path(outPath).parent.mkdir(parents=True, exist_ok=True)
    with open(outPath, "wb") as f:
        f.write(blob.download_blob().readall())


def chunk_text(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    text = text.strip()
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
    start_t = time.time()
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
                "doc_id": doc_id,
                "page": i,
                "chunk_idx": j + 1,
                "content": part
            })
        print(f"📝 Página {i}/{len(pages)} procesada, {len(all_chunks)} chunks hasta ahora.")

    # Guardar chunks en Azure Blob Storage
    blobName = f"contratos/{pdfName}.jsonl"

    # Crear el cliente del blob
    blob = createBlobClient(blobName, CONTAINER_CHUNKS)

    # blob.upload_blob(all_chunks.encode("utf-8"), overwrite=True)
    jsonl_str = "\n".join(json.dumps(x, ensure_ascii=False) for x in all_chunks)
    blob.upload_blob(jsonl_str.encode("utf-8"), overwrite=True)

    print(f"✅ {len(all_chunks)} chunks guardados del archivo {pdfName}")
    print(f"⏱️ Tiempo total: {time.time() - start_t:.2f} s")

if __name__ == "__main__":
    # inicializar logs
    log = get_logger("chunks")
    log.info("Iniciando OCR de PDF")

    #ingresamos el nombre del pdf
    pdf_name = input("Ingresa el nombre del PDF (sin extensión): ").strip()
    try:
        # Obtenemos PDF de azure blob storage
        getPdfFromBlob(pdf_name)

        if not os.path.exists(OUT_PATH):
            raise FileNotFoundError(f"No se encontró {OUT_PATH} en el directorio actual.")
        ocr_pdf_to_chunks(OUT_PATH, pdf_name)
        log.info(f"PDF {pdf_name} procesado exitosamente.")
        print(f"✅ Proceso completado para {pdf_name}.")
    except Exception as e:
        log.error(f"Error procesando PDF {pdf_name}: {e}")
        print(f"❌ Ocurrió un error: {e}")
