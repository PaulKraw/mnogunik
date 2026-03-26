import csv
import random

def make_cities_csv(city_counts, out_path, shuffle=False, seed=42, encoding="utf-8-sig"):
    """
    city_counts: dict[str,int] или список кортежей [("Москва", 3), ("Тверь", 2), ...]
    out_path: путь к csv
    shuffle: перемешать строки перед записью
    seed: фикс для перемешивания
    encoding: 'utf-8-sig' удобно для Excel
    """
    # приведём к последовательности пар в исходном порядке
    items = city_counts.items() if isinstance(city_counts, dict) else list(city_counts)

    rows = []
    for city, n in items:
        n = int(n)
        if n <= 0:
            continue
        rows.extend([str(city)] * n)

    if shuffle:
        random.Random(seed).shuffle(rows)

    with open(out_path, "w", newline="", encoding=encoding) as f:
        w = csv.writer(f)
        w.writerow(["Город"])
        for city in rows:
            w.writerow([city])

    return len(rows)  # вернём, сколько строк записали
