# Create Master Resume

Build a **master resume in JSON** by:
1. Converting multiple PDF/DOCX resumes into a structured JSON schema using **Ollama (local llama3.x)** or **OpenAI API**.
2. Merging all per-file JSONs into **one deduplicated master JSON** (clean skills, unique highlights, chronological work history).

---

## Features
- Recursively scans a folder; if both `.docx` and `.pdf` exist for the same resume, prefers `.docx`.
- Extracts text with **PyMuPDF** (default). Optional **pdfplumber** instructions included.
- Maps resume text → JSON using **Ollama** (`llama3.2`) or **OpenAI** (`gpt-4o-mini`).
- Robust **merge** that avoids crashes (e.g., dicts in highlights) and dedupes by fuzzy matching.
- All paths and settings are in one config file.

---

## Project Structure
- `src/config.py` — edit input/output paths, provider (`ollama` or `openai`), model, API keys.
- `src/resume_extractor.py` — scans PDFs/DOCXs, extracts text, calls LLM, saves JSONs.
- `src/merge_jsons.py` — merges JSONs into `master_resume.json`.
- `examples/resume_schema.json` — reference JSON schema.
- `requirements.txt` — dependencies.

---

## Quickstart

### 1) Install
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
