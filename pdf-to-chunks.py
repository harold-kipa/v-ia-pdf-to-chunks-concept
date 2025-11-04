import os, time, uuid, json, subprocess, shutil, platform
import pytesseract
from pdf2image import convert_from_path

CHUNK_SIZE = 300
CHUNK_OVERLAP = 50

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
    # input("Presiona Enter para continuar...")
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

def ocr_pdf_to_chunks(pdf_path, lang="spa"):
    start_t = time.time()
    doc_id = os.path.basename(pdf_path)
    print(f"📄 Procesando documento: {doc_id}")

    # 1) Intentar texto directo (más rápido/preciso si no es escaneado)
    # direct_txt = extract_text_with_pdftotext(pdf_path)
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
    pages = convert_from_path(pdf_path, dpi=300)  # requiere poppler-utils
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

    out_jsonl = f"{os.path.splitext(doc_id)[0]}_chunks.jsonl"
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for c in all_chunks:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"✅ {len(all_chunks)} chunks guardados en {out_jsonl}")
    print(f"⏱️ Tiempo total: {time.time() - start_t:.2f} s")

if __name__ == "__main__":
    pdf_name = input("Ingresa el nombre del PDF (sin extensión): ").strip()
    pdf_path = f"{pdf_name}.pdf"
    print(f"Procesando {pdf_path}...")
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"No se encontró {pdf_path} en el directorio actual.")
    ocr_pdf_to_chunks(pdf_path)
