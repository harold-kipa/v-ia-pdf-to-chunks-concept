import pytesseract
# print(pytesseract.get_tesseract_version())
import time
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

from pdf2image import convert_from_path
import os


pdf_name = input("ingresa el nombre del pdf: ")
inicio = time.time()

# Ruta al PDF escaneado
pdf_path = f"{pdf_name}.pdf"

# Carpeta temporal para imágenes
temp_dir = "paginas_temp"
os.makedirs(temp_dir, exist_ok=True)

# Convertir PDF a imágenes (una imagen por página)
paginas = convert_from_path(pdf_path, dpi=300)

texto_completo = ""

for i, pagina in enumerate(paginas):
    img_path = os.path.join(temp_dir, f"pagina_{i+1}.png")
    pagina.save(img_path, "PNG")

    # Extraer texto de la imagen usando OCR
    texto = pytesseract.image_to_string(img_path, lang="spa")  # "spa" para español
    texto_completo += f"\n--- Página {i+1} ---\n{texto}"

# Guardar el texto extraído
with open(f"{pdf_name}.txt", "w", encoding="utf-8") as f:
    f.write(texto_completo)

# Detener temporizador
fin = time.time()
tiempo_total = fin - inicio

print(f"Texto extraído y guardado en '{pdf_name}.txt'")
print(f"⏱️ Tiempo total de ejecución: {tiempo_total:.2f} segundos")