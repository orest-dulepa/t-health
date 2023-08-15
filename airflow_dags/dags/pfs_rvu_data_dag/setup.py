from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table_if_not_exists() -> None:
    conn_name = Variable.get('pfs_rvu_db_conn_name')
    hook = PostgresHook(postgres_conn_id=conn_name)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS internal_reference.ref_cms_pfs_rvu (
            hcpcs                                          varchar(20),
            "mod"                                          varchar(5),
            description                                    varchar(255),
            status_code                                    varchar(5),
            not_used_for_medicare_payment                  varchar(5),
            work_rvu                                       double precision,
            non_fac_pe_rvu                                 double precision,
            non_fac_na_indicator                           double precision,
            facility_pe_rvu                                double precision,
            facility_na_indicator                          double precision,
            mp_rvu                                         double precision,
            non_facility_total                             double precision,
            facility_total                                 double precision,
            pctc_ind                                       double precision,
            glob_days                                      varchar(5),
            pre_op                                         double precision,
            intra_op                                       double precision,
            post_op                                        double precision,
            mult_proc                                      double precision,
            bilat_surg                                     double precision,
            asst_surg                                      double precision,
            co_surg                                        double precision,
            team_surg                                      double precision,
            endo_base                                      varchar(5),
            conv_factor                                    double precision,
            physician_supervision_of_diagnostic_procedures varchar(5),
            calculation_flag                               double precision,
            diagnostic_imaging_family_indicator            double precision,
            non_facility_pe_used_for_opps_payment_amount   double precision,
            facility_pe_used_for_opps_payment_amount       double precision,
            eff_start_dt                                   date,
            eff_end_dt                                     date,
            mp_used_for_opps_payment_amount                double precision
        );
    """
    hook.run(create_table_sql)
