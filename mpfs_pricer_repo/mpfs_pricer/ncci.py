from typing import List
from psycopg2 import sql


def get_ncci(db, code_1: str, code_2: str, service_date: str) -> List[dict]:
    query = sql.SQL(
        """
            SELECT * FROM internal_reference.cms_ncci_ptp_practitioner_edits
            WHERE col1 = {code_1}
                AND col2 = {code_2}
                AND effective_date IS NOT NULL
                AND effective_date <= {service_date}
                AND (deletion_date >= {service_date} OR deletion_date IS NULL);
        """
    ).format(
        code_1=sql.Literal(code_1),
        code_2=sql.Literal(code_2),
        service_date=sql.Literal(service_date),
    )

    cursor = db.cursor()
    cursor.execute(query)
    res = cursor.fetchall()

    ncci_data = []

    for ncci_row in res:
        ncci_data.append({
            'col_1': ncci_row[0],
            'col_2': ncci_row[1],
            'effective_date': ncci_row[2],
            'deletion_date': ncci_row[3],
            'modifier': ncci_row[4],
        })

    return ncci_data
