import re
import csv
from collections import Counter
import asyncio
import redis.asyncio as redis


line='A1-10'
cod_gp='31211'
source_date='2025-06-03'
batch_date='2025-06-04'
verified_status='verified'

# === ЭТАП 1: Извлечение кодов из лога и сохранение обрезков ===
log_file = 'syslog'
duplicates_csv = 'duplicates.csv'

code_pattern = re.compile(r'->(.*?)<-')
valid_code_pattern = re.compile(r'^[\w\+\-#:]+$')

codes = []

with open(log_file, 'r', encoding='utf-8') as f:
    for line in f:
        match = code_pattern.search(line)
        if match:
            code = match.group(1).strip()
            if valid_code_pattern.match(code):
                codes.append(code)

code_counts = Counter(codes)

with open(duplicates_csv, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)

    for code, count in code_counts.items():
        if count > 1 and len(code) > 34:
            rest = code[34:]  # пропускаем первые 34 символа
            for i in range(0, len(rest), 34):
                part = rest[i:i+34]
                if len(part) == 34:
                    writer.writerow([part])

print(f'Готово. Разобранные задвоенные коды записаны в {duplicates_csv}.')

# === ЭТАП 2: Обработка в part1 / part2 и запись в processed.csv ===
input_csv = duplicates_csv
processed_csv = 'processed.csv'

with open(input_csv, 'r', encoding='utf-8') as infile, \
     open(processed_csv, 'w', encoding='utf-8', newline='') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    for row in reader:
        if not row:
            continue
        code = row[0].strip()

        code_trimmed = code[3:]  # убираем первые 3 символа

        part1 = code_trimmed[:13]
        part2 = code_trimmed[15:21]
        part3 = code_trimmed[27:].replace('#', '')

        processed_line = f"{part1} {part2} {part3}"
        writer.writerow([processed_line])

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

        reader = csv.reader(infile)
        writer = csv.writer(result_file)
        writer.writerow(['key (part1)', 'value (part2)', 'SADD result'])  # заголовки

        for row in reader:
            if not row:
                continue
            parts = row[0].split()
            if len(parts) >= 2:
                key = parts[0]
                value = parts[1]
                try:
                    result = await r.sadd(key, value)
                    writer.writerow([key, value, result])
                except Exception as e:
                    writer.writerow([key, value, f"ERROR: {e}"])

    await r.close()
    await r.connection_pool.disconnect()


# === ЭТАП 4: Добавление в Postgres из processed.csv с логом в result.csv ===


# === Запуск ===
if __name__ == '__main__':
    asyncio.run(add_all_to_redis())
