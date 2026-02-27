"""CLI helpers for non-bot operation."""
from __future__ import annotations

MENU = {
    "1": 1,
    "2": 2,
    "3": 5,
    "4": 7,
    "5": 12,
    "6": 20,
    "7": 24,
}


def select_hours() -> int:
    print("1 - 1 час\n2 - 2 часа\n3 - 5 часов\n4 - 7 часов\n5 - 12 часов\n6 - 20 часов\n7 - 24 часа")
    choice = input("Выберите период: ").strip()
    return MENU.get(choice, 1)
