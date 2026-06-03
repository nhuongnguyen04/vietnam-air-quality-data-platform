import json
import os

base_dir = os.path.dirname(os.path.abspath(__file__))

# Dynamic JSON loader
vi_path = os.path.join(base_dir, "locale", "vi.json")
en_path = os.path.join(base_dir, "locale", "en.json")

try:
    with open(vi_path, "r", encoding="utf-8") as f:
        vi_trans = json.load(f)
except Exception:
    vi_trans = {}

try:
    with open(en_path, "r", encoding="utf-8") as f:
        en_trans = json.load(f)
except Exception:
    en_trans = {}

TRANSLATIONS = {
    "vi": vi_trans,
    "en": en_trans
}

def t(key, lang="vi"):
    return TRANSLATIONS.get(lang, TRANSLATIONS["vi"]).get(key, key)
