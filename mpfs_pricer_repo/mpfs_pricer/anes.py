from psycopg2 import sql


def get_base_unit(db, cpt, date_of_service):
    query = """
            SELECT
                base_unit
            FROM internal_reference.cms_pfs_anesthesia_base_units
            where
                code = %s and
                beg_eff_date <= %s and
                end_eff_date >= %s

        """

    cursor = db.cursor()
    cursor.execute(query, [cpt, date_of_service, date_of_service])

    res = cursor.fetchone()
    if not res:
        return None

    return res[0]


def get_conversion_factor(db, carrier_to_find, locality_code_to_find, date_of_service):
    query = sql.SQL(
        """
            SELECT
                "Conversion_Factor"
            FROM internal_reference.cms_pfs_anes_conversion_factor
            WHERE
                TRIM(contractor) = {carrier_to_find} AND
                locality = {locality_code_to_find} AND
                beg_eff_date <= {date_of_service} AND
                end_eff_date >= {date_of_service}
        """
    ).format(
        carrier_to_find=sql.Literal(carrier_to_find),
        locality_code_to_find=sql.Literal(locality_code_to_find),
        date_of_service=sql.Literal(date_of_service)
    )

    cursor = db.cursor()
    cursor.execute(query)

    res = cursor.fetchone()
    if not res:
        return None

    return res[0]
