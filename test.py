import csv
import psycopg2
import script
import requests


# тестовая функция чтобы посмотреть sql запрос
def insert_to_postgres_mock():
    with open('processed.csv', 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if not row:
                continue

            # Ожидаем строку: "gtin tail crypto_tail"
            parts = row[0].split()
            if len(parts) != 3:
                print(f"Пропущена строка: {row}")
                continue

            gtin, tail, crypto_tail = parts
            tail = tail.replace("'", "''")

            # Формируем текст запроса для вывода
            sql_text = f"""
                INSERT INTO marking.line 
                (line, cod_gp, gtin, tail, crypto_tail, source_date, batch_date, verified_status)
                VALUES ('{script.line_number}', '{script.cod_gp}', '{gtin}', '{tail}', '{crypto_tail}', 
                        '{script.source_date}', '{script.batch_date}', '{script.verified_status}');
                """
            print(sql_text.strip())  # выводим запрос в консоль
    print(gtin)
    send_post_to_116(gtin)


def insert_to_postgres():
    conn = psycopg2.connect(
        host='10.10.3.109',
        database='molzavod',
        user='postgres',
        password='111111'
    )
    cursor = conn.cursor()

    with open('processed.csv', 'r', encoding='utf-8') as infile:
        for line in infile:
            line = line.strip()  # убираем переносы строк
            if not line:
                continue

            # Ожидаем строку: "gtin tail crypto_tail"
            parts = line.split()
            if len(parts) != 3:
                print(f"Пропущена строка: {line}")
                continue

            gtin, tail, crypto_tail = parts

            try:
                cursor.execute(
                    """
                    INSERT INTO marking.line 
                    (line, cod_gp, gtin, tail, crypto_tail, source_date, batch_date, verified_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (script.line_number, script.cod_gp, gtin, tail, crypto_tail, script.source_date, script.batch_date, script.verified_status)
                )
            except Exception as e:
                print(f"Ошибка при вставке строки {line}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Данные успешно записаны в PostgreSQL.")
    print("gtin = ", gtin)
    send_post_to_116(gtin)

def send_post_to_116(gtin):
    url = "http://10.10.3.116:6200/request/marking/marking_line/MarkingLine/get_info_line?session=15"

    payload = {
    "markstation_id": script.line_number,
    "source_date": script.source_date,
    "good_codes": script.counter[gtin],
    "defect_codes": 0,
    "total_codes": script.counter[gtin],
    "duplicates_codes": 0,
    "current_gtin": gtin,
    "current_cod_gp": script.cod_gp,
    "current_batch_date": script.batch_date,
    "plc_state": {}
    }

    print(payload)

    # POST запрос
    response = requests.post(
        url,
        json=payload
    )

    # Вывод результата
    print(f"STATUS: {response.status_code}")
    print(response.text)

if __name__ == '__main__':
    insert_to_postgres()
