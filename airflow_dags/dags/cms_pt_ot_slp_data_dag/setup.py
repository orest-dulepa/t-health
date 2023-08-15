from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table_if_not_exists() -> None:
    conn_name = Variable.get('cms_pt_ot_slp_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS internal_reference.ref_cms_opps_pt_ot_services (
            code                  varchar(5),
            short_descriptor      varchar(28),
            carrier               integer,
            locality              integer,
            fee_amount            double precision,
            half_of_reduction     double precision,
            practice_expense_rvus double precision,
            eff_start_dt          date,
            eff_end_dt            date
        );
    """
    hook.run(create_table_sql)
