# Keyword Brands Pipeline

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export KEYWORDTOOL_KEY=...
export KEYAPP_KEY=...
export APPSTORESPY_KEY=...
python3 run_pipeline.py --country br --caps 500000 --sort desc --sort-by volume
```

### Одноразовый запуск с ключами в командной строке

Если вы предпочитаете передавать ключи без предварительного экспорта в окружение,
вы можете указать их непосредственно перед запуском команды:

```bash
KEYWORDTOOL_KEY=... \
KEYAPP_KEY=... \
APPSTORESPY_KEY=... \
python3 run_pipeline.py \
  --country br \
  --metrics-network googlesearch \
  --caps 500000 \
  --sort desc --sort-by volume \
  --only-nonused \
  --audit-topn 6
```

Обратите внимание, что обратный слеш (`\`) в конце строки позволяет переносить
команду на новую строку в оболочке Bash. Если вы запускаете всё в одну строку,
переносы не нужны, достаточно разделить аргументы пробелами.
