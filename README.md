## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

```bash
python src/download_graph.py [--workdir DIR] [--batch-size N] \
    [--max-retries K] [--retry-delay SEC]
```