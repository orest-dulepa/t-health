from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table_if_not_exists() -> None:
    conn_name = Variable.get('opps_addendum_b_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS internal_reference.ref_cms_opps_addendum_b (
            hcpcs                                             varchar(18),
            short_descriptor                                  varchar(29),
            si                                                varchar(3),
            apc                                               integer,
            relative_weight                                   double precision,
            payment_rate                                      varchar(14),
            national_unadjusted_copayment                     varchar(9),
            minimum_unadjusted_copayment                      varchar(11),
            notes                                             varchar(1),
            drug_pass_through_expiration_during_calendar_year varchar(1),
            asterix_indicates_change                          varchar(1),
            eff_start_dt                                      date,
            eff_end_dt                                        date
        );
    """
    hook.run(create_table_sql)
