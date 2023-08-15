from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table_if_not_exists() -> None:
    conn_name = Variable.get('taxonomy_code_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS internal_reference.ref_nucc_taxonomy_code (
            "date"             date,
            code               varchar(20),
            "grouping"         varchar(255),
            classification     varchar(255),
            specialization     varchar(255),
            definition         text,
            effective_date     date,
            deactivation_date  date,
            last_modified_date date,
            notes              text,
            display_name       varchar(255),
            section            varchar(20)
        );
    """
    hook.run(create_table_sql)
