import logging

import requests
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def create_table_if_not_exists() -> None:
    conn_name = Variable.get('wage_index_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    create_table_sql = """
            CREATE TABLE IF NOT EXISTS internal_reference.ref_cms_wage_index_by_cbsa (
                year                 integer,
                version              double precision,
                cbsa                 integer,
                area_name            varchar(250),
                state                varchar(2),
                state_code           integer,
                hourly_wage          varchar(100),
                hourly_wage_3yr      varchar(100),
                wage_index           double precision,
                gaf                  double precision,
                reclass_wi           varchar(100),
                reclass_gaf          varchar(100),
                frontier_wage_elig   varchar(100),
                rural_floor_elig     varchar(100),
                pre_floor_wi         varchar(100),
                reclass_frontier_wi  varchar(100),
                reclass_rural_wi     varchar(100),
                reclass_pre_floor_wi varchar(100)
            );
        """
    hook.run(create_table_sql)


def check_url() -> None:
    """
    # Check if url is reachable
    """

    url = 'https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/AcuteInpatientPPS/Wage-Index-Files'

    logger.info(f'Check if url {url} is reachable')
    requests.head(url).raise_for_status()
    logger.info(f'Url {url} - Ok')
