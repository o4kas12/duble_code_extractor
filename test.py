import csv
import psycopg2

def insert_to_postgres():
    conn = psycopg2.connect(
        host='10.10.3.109',
        database='molzavod',
        user='postgres',
        password='111111'
    )
    cursor = conn.cursor()

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

            try:
                cursor.execute(
                    """
                    INSERT INTO marking.line 
                    (line, cod_gp, gtin, tail, crypto_tail, source_date, batch_date, verified_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    ('A1-10', '31211', gtin, tail, crypto_tail, '2025-06-03', '2025-06-04', 'verified')
                )
            except Exception as e:
                print(f"Ошибка при вставке строки {row}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("Данные успешно записаны в PostgreSQL.")

if __name__ == '__main__':
    insert_to_postgres()
