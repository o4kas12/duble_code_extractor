import re
import csv
from collections import Counter
import asyncio
import redis.asyncio as redis


line_number='TERM1'
cod_gp='30141'
source_date='2026-02-05'
batch_date='2026-02-06'
verified_status='verified'

# === ЭТАП 1: Извлечение кодов из лога и сохранение обрезков ===
log_file = 'syslogs/syslog_A1-07'
duplicates_csv = 'duplicates.csv'

code_pattern = re.compile(
    r'receive on 2000\s+->(.*?)<-'
)

with open(log_file, 'r', encoding='utf-8') as f, \
     open(duplicates_csv, 'w', encoding='utf-8', newline='') as csvfile:

    for line in f:
        match = code_pattern.search(line)
        if not match:
            continue

        code = match.group(1).strip()
        length = len(code)

        # Пропускаем первый блок 34 символа
        if length <= 34:
            continue  # если всего меньше или ровно 34 — пропускаем всё

        for i in range(34, length, 34):  # начинаем с 34, чтобы пропустить первый код
            part = code[i:i+34]
            csvfile.write(part + '\n')



print(f'Готово. Результат записан в {duplicates_csv}')

# === ЭТАП 2: Обработка в part1 / part2 и запись в processed.csv ===
input_csv = duplicates_csv
processed_csv = 'processed.csv'

with open(input_csv, 'r', encoding='utf-8') as infile, \
     open(processed_csv, 'w', encoding='utf-8') as outfile:

    for line in infile:
        code = line.strip()
        if len(code) != 34:
            print(f'⚠️ Пропуск блока не 34 символа: {code}')
            continue

        code_trimmed = code[3:]  # убираем первые 3 символа

        part1 = code_trimmed[:13]
        part2 = code_trimmed[15:21]
        part3 = code_trimmed[27:].replace('#', '')

        processed_line = f"{part1} {part2} {part3}"
        outfile.write(processed_line + '\n')

print(f"Обработка завершена, результат записан в {processed_csv}")

# === ЭТАП 3: Добавление в Redis из processed.csv с логом в result.csv ===
async def add_all_to_redis():
    r = redis.Redis(
        host='192.168.100.122',
        port=6379,
        password='Admin61325!',
        decode_responses=True
    )

    with open(processed_csv, 'r', encoding='utf-8') as infile, \
         open('result.csv', 'w', encoding='utf-8', newline='') as result_file:

        # заголовок
        result_file.write('key (part1),value (part2),SADD result\n')

        for line in infile:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if len(parts) >= 2:
                key = parts[0]
                value = parts[1]

                try:
                    # настоящий SADD
                    result = await r.sadd(key, value)
                    result_file.write(f"{key},{value},{result}\n")
                except Exception as e:
                    result_file.write(f"{key},{value},ERROR: {e}\n")

    await r.close()
    await r.connection_pool.disconnect()


# === ЭТАП 4: Добавление в Postgres из processed.csv с логом в result.csv ===


# === Запуск ===
if __name__ == '__main__':
    asyncio.run(add_all_to_redis())