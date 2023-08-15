from pathlib import Path


def get_zplc_data_file():
    full_path = Path(__file__).parent / "data" / "zplc" / "ZIP5.txt"

    if not full_path.exists():
        raise ValueError("No ZPLC regional location data found.")

    return full_path


def find_region_by_zip(zip):
    if zip is None:
        return None

    if not isinstance(zip, str):
        raise ValueError("Invalid zipcode data.")

    zip = zip[0:5]

    file = get_zplc_data_file()

    with open(file) as f:
        for row in f:
            row_zip, carrier, locality = row[2:7], row[7:12], row[12:14]
            if row_zip == zip:
                return {
                    "zip": row_zip,
                    "carrier": carrier,
                    "locality": locality,
                }

    return None
