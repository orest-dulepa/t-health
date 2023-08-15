def get_gpci(db, carrier_to_find, locality_code_to_find, date_of_service):
    query = """
            SELECT
                "Medicare Administrative Contractor", "Locality Number", "Locality Name", "PW GPCI",
                "PE GPCI", "MP GPCI", eff_start_dt, eff_end_dt
            FROM internal_reference.cms_gpci
            where
                "Medicare Administrative Contractor" = %s and
                "Locality Number" = %s and
                eff_start_dt <= %s and
                eff_end_dt >= %s

        """

    cursor = db.cursor()
    cursor.execute(query, [carrier_to_find, locality_code_to_find, date_of_service, date_of_service])

    res = cursor.fetchone()
    if not res:
        return None

    carrier = res[0]
    locality_code = res[1]
    locality_name = res[2]
    pw_gpci = res[3]
    pe_gpci = res[4]
    mp_gpci = res[5]

    found_data = {
        "carrier": carrier,
        "locality_code": locality_code,
        "locality_name": locality_name,
        "pw_gpci": pw_gpci,
        "pe_gpci": pe_gpci,
        "mp_gpci": mp_gpci,
    }

    return found_data
