# duble_code_extractor

## Описание

Скрипты для извлечения, обработки и загрузки кодов из логов в Redis и PostgreSQL.

## Структура

- `script.py` — основной скрипт для обработки логов, поиска дублей, подготовки данных и загрузки в Redis.
- `test.py` — скрипт для загрузки данных из `processed.csv` в PostgreSQL.
- `syslog` — исходный лог-файл.
- `duplicates.csv` — промежуточный CSV с найденными дублирующимися кодами.
- `processed.csv` — итоговый CSV для загрузки в БД.
- `result.csv` — лог загрузки в Redis.
- `requirements.txt` — зависимости проекта.

## Установка

```sh
pip install -r requirements.txt
```

## Использование

1. Поместите файл лога (`syslog`) в корень проекта.
2. Запустите обработку и загрузку в Redis:
    ```sh
    python script.py
    ```
3. Для загрузки в PostgreSQL:
    ```sh
    python test.py
    ```

## Зависимости

- Python 3.8+
- psycopg2
- redis (asyncio)

## Настройки

Параметры подключения к Redis и PostgreSQL задаются в файлах [`script.py`](script.py) и [`test.py`](test.py).