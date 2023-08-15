import math
import dateutil.parser
from datetime import date


def fit_date(val: str) -> str:
    if val is None:
        raise ValueError("Invalid date - Date can't be None")

    val = dateutil.parser.parse(val).date().isoformat()
    formatted_val = val.replace("-", "")

    assert (len(formatted_val) == 8)

    return formatted_val


def to_currency(value: float) -> float:
    return round(value, 2)


def get_quarter(date_text: str) -> str:
    date = dateutil.parser.parse(date_text)
    return f"{date.year}_Q{math.ceil(date.month / 3)}"


def get_year(date_text: str) -> str:
    date = dateutil.parser.parse(date_text)
    return f"{date.year}"


def is_date_between_dates(service_date: date, date_1: date, date_2: date):
    if None in [service_date, date_1]:
        return False

    if date_2 is None and service_date > date_1:
        return True

    return date_1 <= service_date and (date_2 is None or service_date <= date_2)
