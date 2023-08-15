"""Load CSV files downloaded from SFTP archives to the Output DB tables"""
import logging
from zipfile import ZipFile, ZipInfo
from contextlib import closing

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.utils.xcom import get_return_value_from_xcom


logger = logging.getLogger(__name__)
SCHEMA = "definitive_healthcare"
CONN_NAME = Variable.get(
    "dh_db_conn_name", default_var="postgres_t_data"
)
# Certain output tables have additional columns defined (ID, geom), we need
# to pass table columns for these tables when COPY explicitly to not fail
# and auto-populate ID's, all other CSV file are matched with the output table
# schema
UNMATCHED_SCHEMA = {
    "hospital_facility_affiliations": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "hq_city",
        "hq_state",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
    ),
    "hospital_overview": (
        "hospital_id",
        "hospital_name",
        "provider_number",
        "tax_id",
        "firm_type",
        "hospital_type",
        "idn_integration_level",
        "company_status",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_county",
        "cbsa_code",
        "cbsa_population_est_most_recent",
        "cbsa_population_growth_most_recent",
        "hq_latitude",
        "hq_longitude",
        "website",
        "hq_phone",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "npi_number",
        "program_340b_id",
        "public_corporation",
        "stock_symbol",
        "pos_medical_school_affiliation",
        "medical_school_affiliates",
        "accreditation_agency",
        "hospital_ownership",
        "market_concentration_index",
        "medicare_administrative_contractor",
        "financial_data_date",
        "idn_financial_data_reporting_status",
        "gpo_affiliations",
        "primary_gpo_id",
        "primary_gpo_name",
        "aco_affiliations",
        "hie_affiliations",
        "dhc_profile_link",
        "geographic_classification",
        "network_firm_type",
        "network_parent_firm_type",
        "hospital_compare_overall_rating",
        "hcahps_summary_star_rating",
        "_340b_hospital_type",
        "hq_region",
    ),
    "imaging_center_connected_care_affiliations": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_aco_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
    ),
    "imaging_center_facility_affiliations": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "hq_city",
        "hq_state",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
    ),
    "imaging_center_overview": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "imaging_center_type",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_phone",
        "hq_region",
        "hq_county",
        "hq_latitude",
        "hq_longitude",
        "cbsa_code",
        "cbsa_population_est_most_recent",
        "cbsa_population_growth_most_recent",
        "website",
        "dhc_profile_link",
        "npi_number",
        "primary_taxonomy_code",
        "primary_taxonomy",
        "secondary_taxonomy",
        "company_status",
        "network_id",
        "network_name",
        "network_firm_type",
        "network_parent_id",
        "network_parent_name",
        "hospital_parent_id",
        "hospital_parent_name",
        "pg_parent_id",
        "pg_parent_name",
        "gpo_affiliations",
        "number_member_imaging_centers",
        "medicare_number_procedures",
        "medicare_pmts",
        "medicare_charges",
        "medicare_allowed_amt",
        "network_parent_firm_type",
    ),
    "imaging_center_services_provided": (
        "hospital_id",
        "hospital_name",
        "service",
        "provided",
    ),
    "physician_groups_facility_affiliations": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "hq_city",
        "hq_state",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
    ),
    "physician_groups_overview": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "pg_type",
        "main_specialty",
        "other_specialties",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_county",
        "hq_phone",
        "hq_latitude",
        "hq_longitude",
        "cbsa_code",
        "website",
        "npi_number",
        "group_practice_pac_id",
        "hospital_parent_id",
        "hospital_parent_name",
        "number_physicians_pg",
        "number_group_practice_members",
        "network_firm_type",
        "network_parent_firm_type",
        "main_specialty_group",
        "hq_region",
    ),
    "physician_groups_practice_locations": (
        "hospital_id",
        "hospital_name",
        "location_id",
        "location_name",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_phone",
        "primary_location",
        "hq_latitude",
        "hq_longitude",
    ),
    "physicians_facility_affiliations_current": (
        "npi",
        "first_name",
        "last_name",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
        "affiliated_hq_latitude",
        "affiliated_hq_longitude",
        "network_id",
        "network_name",
        "primary_affiliated_facility_flag",
    ),
    "physicians_medical_board_orders_and_actions": (
        "npi",
        "first_name",
        "last_name",
        "reporting_entity",
        "order_date",
        "action_description",
        "basis_description",
        "term_length",
    ),
    "physicians_overview": (
        "npi",
        "pac_id",
        "first_name",
        "middle_name",
        "last_name",
        "suffix",
        "gender",
        "credential",
        "specialty_primary",
        "specialty_secondary",
        "medical_school_name",
        "graduation_year",
        "age",
        "hospitalist_flag",
        "primary_affiliation_hospital_id",
        "primary_affiliation_hospital_name",
        "primary_affiliation_network_id",
        "primary_affiliation_network_name",
        "primary_affiliation_network_parent_id",
        "primary_affiliation_network_parent_name",
        "primary_practice_location_name",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_latitude",
        "hq_longitude",
        "hq_phone",
        "cbsa_code",
        "primary_taxonomy",
        "primary_practice_location_id",
        "primary_practice_pg_id",
        "participates_in_mips",
        "participated_in_clinical_trials",
        "specialty_primary_group",
        "role_name",
    ),
    "physicians_practice_locations": (
        "npi",
        "first_name",
        "last_name",
        "pg_id",
        "location",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_phone",
        "do_not_call_phone",
        "primary_location",
        "hq_fax_number",
        "hq_latitude",
        "hq_longitude",
        "location_id",
        "address_id",
    ),
    "surgery_centers_connected_care_affiliations": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_aco_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
    ),
    "surgery_centers_facility_affiliations": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "hq_city",
        "hq_state",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "affiliated_id",
        "affiliated_name",
        "affiliated_firm_type",
        "affiliated_hq_city",
        "affiliated_hq_state",
    ),
    "surgery_centers_overview": (
        "hospital_id",
        "hospital_name",
        "firm_type",
        "network_id",
        "network_name",
        "network_parent_id",
        "network_parent_name",
        "hq_address",
        "hq_address1",
        "hq_city",
        "hq_state",
        "hq_zip_code",
        "hq_county",
        "hq_phone",
        "hq_latitude",
        "hq_longitude",
        "cbsa_code",
        "cbsa_population_est_most_recent",
        "cbsa_population_growth_most_recent",
        "website",
        "dhc_profile_link",
        "provider_number",
        "npi_number",
        "accreditation_agency",
        "number_operating_rooms",
        "hospital_parent_id",
        "hospital_parent_name",
        "pg_parent_id",
        "pg_parent_name",
        "group_practice_pac_id",
        "gpo_affiliations",
        "aco_affiliations",
        "hie_affiliations",
        "cin_affiliations",
        "company_status",
        "number_procedures",
        "total_charges",
        "avg_charge_per_procedure",
        "national_avg_charge_per_procedure",
        "state_avg_charge_per_procedure",
        "geographic_classification",
        "network_firm_type",
        "network_parent_firm_type",
        "hq_region",
    ),
    "surgery_centers_types_of_procedures_provided": (
        "hospital_id",
        "hospital_name",
        "type_of_procedure",
        "provided",
    ),
}


def truncate_output_tables() -> None:
    """Truncates all tables in the 'definitive_healthcare' schema"
    by calling `truncate_dh_tables` predefined DB function.
    Tables need to be truncated before we start to load new data into them.
    """
    hook = PostgresHook(postgres_conn_id=CONN_NAME)

    truncate_tables_sql = "SELECT definitive_healthcare.truncate_dh_tables();"
    hook.run(truncate_tables_sql)


def load_file(
    archive: ZipFile, in_file: ZipInfo, hook: PostgresHook, file_name: str
) -> None:
    """COPY in_file CSV from the archive to the output DB"""

    with archive.open(in_file.filename) as data_file:
        logger.info("Loading %s", in_file.filename)
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                table_name = file_name.split(".csv")[0]
                target_spec = f"{SCHEMA}.{table_name}"
                copy_sql = f"COPY {target_spec}"
                if table_name in UNMATCHED_SCHEMA:
                    columns = UNMATCHED_SCHEMA[table_name]
                    columns_string = ",".join(columns)
                    copy_sql = f"{copy_sql} ({columns_string})"
                sql = (
                    f"{copy_sql} FROM STDIN " "WITH DELIMITER ',' CSV HEADER;"
                )
                logger.info(sql)
                cursor.copy_expert(sql, data_file)
                conn.commit()


def load_data_to_db(**context: dict) -> None:
    """Reads the data from zip files from S3 and loads the data to output
    tables. Each ZIP file contains a bunch of .csv files we want to upload to
    the DB. The output table is the name of CSV file without the extension.
    Output tables schema is pre created."""
    hook = PostgresHook(postgres_conn_id=CONN_NAME)
    s3_hook = S3Hook()
    s3_url = get_return_value_from_xcom("set_up_s3_bucket_for_", **context)
    bucket, key = s3_hook.parse_s3_url(s3_url)
    zip_files = s3_hook.list_keys(bucket, key)

    for zip_path in zip_files:
        fname = zip_path.split("/")[-1]
        with ZipFile(s3_hook.download_file(s3_url + "/" + fname)) as archive:
            for in_file in archive.filelist:
                file_name = in_file.filename.lower()
                if not file_name.endswith(".csv"):
                    logger.warning("Skipping %s", file_name)
                    continue
                load_file(archive, in_file, hook, file_name)
