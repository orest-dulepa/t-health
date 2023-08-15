def find_zip_by_npi(db, npi):
    query = """
    SELECT "Provider Business Practice Location Address Postal Code" FROM internal_reference.cms_nppes_npidata_pfile_20210411
    where npi = %s
    """

    cursor = db.cursor()
    cursor.execute(query, [npi])

    for row in cursor:
        return row[0]

    return None


def find_tc_by_npi(db, npi):
    query = """
    SELECT "Healthcare Provider Taxonomy Code_1" FROM internal_reference.cms_nppes_npidata_pfile_20210411
    where npi = %s
    """

    cursor = db.cursor()
    cursor.execute(query, [npi])

    for row in cursor:
        return row[0]

    return None
