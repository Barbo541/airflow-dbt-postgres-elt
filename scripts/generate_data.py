"""Generate original, deterministic synthetic pet records; no external input data."""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1] / "dockerized_dbt_postgresql_etl/dbt-PostgreSQL-ELT"


def generate(destination):
    rng = random.Random(20260904)
    destination.mkdir(parents=True, exist_ok=True)
    users = [
        (1000 + i // 2, i, rng.choice(["18-25", "26-35", "36-45", "46-55", "56-65", "66+"]),
         rng.choice(["Cat", "Dog"]))
        for i in range(1, 523)
    ]
    health = [
        (rng.randint(1, 522), date(2025, 1, 1) + timedelta(days=rng.randrange(180)),
         rng.choice(["Routine check", "Dental check", "Vaccination", "Skin irritation"]),
         rng.choice(["Monitoring", "Follow-up scheduled", "Resolved"]))
        for _ in range(940)
    ]
    activities = [
        (rng.randint(1, 522), date(2025, 1, 1) + timedelta(days=rng.randrange(180)),
         rng.choice(["Walk", "Play", "Rest", "Running"]),
         "-" if i % 19 == 0 else str(rng.randint(0, 120)))
        for i in range(938)
    ]
    for filename, columns, rows in [
        ("users.csv", ["owner_id", "pet_id", "owner_age_group", "pet_type"], users),
        ("pet_health.csv", ["pet_id", "visit_date", "issue", "resolution"], health),
        ("pet_activities.csv", ["pet_id", "date", "activity_type", "duration_minutes"], activities),
    ]:
        with (destination / filename).open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows(rows)


if __name__ == "__main__":
    generate(PROJECT / "raw_data")
