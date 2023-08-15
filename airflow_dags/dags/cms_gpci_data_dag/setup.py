from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table_if_not_exists() -> None:
    conn_name = Variable.get('cms_gpci_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS internal_reference.ref_cms_gpci (
            carrier         varchar(15),
            state           varchar(2),
            locality_number integer,
            locality_name   varchar(128),
            pw_gpci         double precision,
            pe_gpci         double precision,
            mp_gpci         double precision,
            eff_start_dt    date,
            eff_end_dt      date
        );
    """
    hook.run(create_table_sql)
