from pathlib import Path
import os

# ===== Edit these paths =====
INPUT_DIR = Path("./resumes")          # Folder with PDFs/DOCXs OR existing JSONs
OUTPUT_DIR = Path("./out_resume_json") # Where to write per-file JSONs and the master

# Extraction provider/model
PROVIDER = "ollama"          # or "openai"
MODEL = "llama3.2"           # (OpenAI example: "gpt-4o-mini")
OLLAMA_HOST = "http://localhost:11434"

# OpenAI API key: prefer environment variable in practice (avoid committing secrets)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")  # leave blank or set via env
# Auto-merge after extraction
MERGE_AFTER_EXTRACTION = True
