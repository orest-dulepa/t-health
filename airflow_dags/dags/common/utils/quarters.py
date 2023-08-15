from datetime import date
from calendar import monthrange


def get_date_of_first_day_of_quarter(year: int, quarter: int) -> date:
    first_month_of_quarter = 3 * quarter - 2
    date_of_first_day_of_quarter = date(year, first_month_of_quarter, 1)

    return date_of_first_day_of_quarter


def get_date_of_last_day_of_quarter(year: int, quarter: int) -> date:
    last_month_of_quarter = 3 * quarter
    date_of_last_day_of_quarter = date(year, last_month_of_quarter, monthrange(year, last_month_of_quarter)[1])

    return date_of_last_day_of_quarter
