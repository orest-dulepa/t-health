from typing import List

from common.utils.load import format_sql_row_item

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_pfs_rvu_to_db(**context) -> None:
    pfs_rvu_data: List[dict] = context['ti'].xcom_pull(task_ids='main__get_all_pfs_rvu__task')

    if len(pfs_rvu_data) == 0:
        return

    conn_name = Variable.get('pfs_rvu_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    sql_values = ''

    for pfs_rvu_data_item in pfs_rvu_data:
        for pfs_rvu_row in pfs_rvu_data_item['data']:
            pfs_rvu_row = [format_sql_row_item(pfs_rvu_row_item) for pfs_rvu_row_item in pfs_rvu_row]

            sql_values += '('
            sql_values += ', '.join(pfs_rvu_row)
            sql_values += f", '{pfs_rvu_data_item['date_of_first_day_of_quarter']}'"
            sql_values += f", '{pfs_rvu_data_item['date_of_last_day_of_quarter']}'"
            sql_values += '), '

    sql_values = sql_values[:-2]
    sql_values += ';'

    sql = f"""
        INSERT INTO internal_reference.ref_cms_pfs_rvu (
            hcpcs, "mod", description, status_code, not_used_for_medicare_payment, work_rvu, non_fac_pe_rvu,
            non_fac_na_indicator, facility_pe_rvu, facility_na_indicator, mp_rvu, non_facility_total,
            facility_total, pctc_ind, glob_days, pre_op, intra_op, post_op, mult_proc, bilat_surg, asst_surg,
            co_surg, team_surg, endo_base, conv_factor, physician_supervision_of_diagnostic_procedures,
            calculation_flag, diagnostic_imaging_family_indicator, non_facility_pe_used_for_opps_payment_amount,
            facility_pe_used_for_opps_payment_amount, mp_used_for_opps_payment_amount, eff_start_dt, eff_end_dt
        ) VALUES {sql_values}
    """

    hook.run(sql)
