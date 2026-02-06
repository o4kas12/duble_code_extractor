import csv
import psycopg2
import script

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

if __name__ == '__main__':
    insert_to_postgres()
