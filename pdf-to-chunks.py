import os, time, uuid, json
import pytesseract
from pdf2image import convert_from_path

# Configura Tesseract si no está en PATH
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

CHUNK_SIZE = 4000     # aprox. 500–1000 tokens según el texto
CHUNK_OVERLAP = 600   # ~15% de solape

def chunk_text(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    text = text.strip()
    if not text: 
        return []
    chunks, start = [], 0
    while start < len(text):
        end = min(start + size, len(text))
        # intenta cortar en fin de párrafo/oración si es posible
        cut = max(text.rfind("\n\n", start, end), text.rfind(". ", start, end))
        if cut == -1 or cut < start + int(size * 0.5):
            cut = end
        chunks.append(text[start:cut].strip())
        if cut == len(text): 
            break
        start = max(0, cut - overlap)
    return chunks

def ocr_pdf_to_chunks(pdf_path, lang="spa"):
    start_t = time.time()
    doc_id = os.path.basename(pdf_path)
    pages = convert_from_path(pdf_path, dpi=300)
    all_chunks = []
    for i, img in enumerate(pages, start=1):
        txt = pytesseract.image_to_string(img, lang=lang)
        for j, part in enumerate(chunk_text(txt)):
            all_chunks.append({
                "id": str(uuid.uuid4()),
                "doc_id": doc_id,
                "page": i,
                "chunk_idx": j + 1,
                "content": part
            })
    out_jsonl = f"{os.path.splitext(doc_id)[0]}_chunks.jsonl"
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for c in all_chunks:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"{len(all_chunks)} chunks guardados en {out_jsonl}")
    print(f"Tiempo total: {time.time() - start_t:.2f} s")

if __name__ == "__main__":
    pdf_name = input("Ingresa el nombre del PDF (sin extensión): ").strip()
    ocr_pdf_to_chunks(f"{pdf_name}.pdf")
