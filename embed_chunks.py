import sys, json
from sentence_transformers import SentenceTransformer

MODEL_NAME = "intfloat/multilingual-e5-small"  # 384 dimensiones

def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def write_jsonl(path, items):
    with open(path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def main(in_path, out_path, batch_size=64):
    model = SentenceTransformer(MODEL_NAME)
    buf_in, buf_out = [], []

    def flush_batch():
        if not buf_in:
            return
        # E5: prefijo "passage: " para documentos
        texts = [f"passage: {x['content']}" for x in buf_in]
        vecs = model.encode(texts, normalize_embeddings=True)  # recomendado
        for x, v in zip(buf_in, vecs):
            x["content_vector"] = [float(z) for z in v.tolist()]  # listo p/ Azure
            buf_out.append(x)
        buf_in.clear()

    for obj in read_jsonl(in_path):
        buf_in.append(obj)
        if len(buf_in) >= batch_size:
            flush_batch()

    flush_batch()
    write_jsonl(out_path, buf_out)
    print(f"OK → {len(buf_out)} chunks con embeddings (dim={len(buf_out[0]['content_vector']) if buf_out else 'N/A'})")
    print(f"Salida: {out_path}")

if __name__ == "__main__":
    # Uso: python embed_chunks.py chunks.jsonl chunks_with_vectors.jsonl
    in_file  = sys.argv[1] if len(sys.argv) > 1 else "PARTE_GENERAL.jsonl"
    out_file = sys.argv[2] if len(sys.argv) > 2 else "PARTE_GENERAL_vectors.jsonl"
    main(in_file, out_file)
