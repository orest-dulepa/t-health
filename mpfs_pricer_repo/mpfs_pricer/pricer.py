from datetime import datetime
from typing import List, Tuple, Union
from mpfs_pricer.anes import get_base_unit, get_conversion_factor
from mpfs_pricer.payment_type import requires_facilty_payment
from mpfs_pricer.data_files import find_region_by_zip
from mpfs_pricer.rvu import get_rvus
from mpfs_pricer.gpci import get_gpci
from mpfs_pricer.ncci import get_ncci
from mpfs_pricer.nppes import find_zip_by_npi, find_tc_by_npi
from mpfs_pricer.adjustments import perform_adjustments, perform_adjustments_multiple, \
    perform_adjustments_bilateral_surgery, perform_adjustments_anesthesia_pricing
from mpfs_pricer.database import get_db, get_db_config_from_env
from mpfs_pricer.utils import get_quarter, fit_date, to_currency


def price_line_item_get(
        data_item: Tuple[dict, dict, dict, str, Union[float, None], Union[float, None]],
        data_item_index: int = 0,
        ncci_info: List[List[dict]] = []):
    # Inputs
    line_item, gpci_info, rvus, provider_taxonomy_code, anes_base_unit, anes_conversion_factor = data_item
    date_of_service = line_item['service_date']
    place_of_service = line_item['place_of_service']
    cpt = line_item['code']
    mod1 = line_item['mod1']
    mod2 = line_item['mod2']
    mod3 = line_item['mod3']
    mod4 = line_item['mod4']
    charges = float(line_item['charges'])
    units = float(line_item['quantity'])

    # Calculated fields
    comments = []
    service_quarter = get_quarter(date_of_service)

    pw_gpci = float(gpci_info["pw_gpci"])
    pe_gpci = float(gpci_info["pe_gpci"])
    mp_gpci = float(gpci_info["mp_gpci"])
    locality_name = gpci_info["locality_name"]

    facilty_payment = requires_facilty_payment(place_of_service)

    if rvus is None:
        comments.append("Non-Payable code")
        wrvu = 0.0
        mp_rvu = 0.0
        pe_rvu = 0.0
        conversion_factor = 0.0
        global_days = ""
        asst_surg = 9
        co_surg = 9
        team_surg = 9
        multi_proc = 9
        bilat_surg = 9
        pre_op = 0.0
        intra_op = 0.0
        post_op = 0.0
        endo_base = ""
    else:
        wrvu = float(rvus["work_rvu"])
        mp_rvu = float(rvus["mp_rvu"])
        if facilty_payment:
            pe_rvu = float(rvus["fac_pe_rvu"])
        else:
            pe_rvu = float(rvus["nonfac_pe_rvu"])
        asst_surg = int(rvus["asst_surg"])
        co_surg = int(rvus["co_surg"])
        team_surg = int(rvus["team_surg"])
        multi_proc = int(rvus["multi_proc"])
        bilat_surg = int(rvus["bilat_surg"])
        pre_op = float(rvus["pre_op"])
        intra_op = float(rvus["intra_op"])
        post_op = float(rvus["post_op"])
        conversion_factor = float(rvus["conv_factor"])
        global_days = rvus["glob_days"]
        endo_base = rvus["endo_base"]

    # Main Payment Calculation
    payment = ((wrvu * pw_gpci) + (pe_rvu * pe_gpci) + (mp_rvu * mp_gpci)) * conversion_factor * units

    line_item_payment_details = {
        "code": cpt,
        "mod1": mod1,
        "mod2": mod2,
        "mod3": mod3,
        "mod4": mod4,
        "charges": charges,
        "quantity": units,
        "locality_name": locality_name,
        "quarter": service_quarter,
        "wrvu": wrvu,
        "pe_rvu": pe_rvu,
        "mp_rvu": mp_rvu,
        "pw_gpci": pw_gpci,
        "pe_gpci": pe_gpci,
        "mp_gpci": mp_gpci,
        "conversion_factor": conversion_factor,
        "global_surgery_code": global_days,
        "line_item_payment": payment,
        "comments": comments,
        "asst_surg": asst_surg,
        "co_surg": co_surg,
        "team_surg": team_surg,
        "multi_proc": multi_proc,
        "bilat_surg": bilat_surg,
        "pre_op": pre_op,
        "intra_op": intra_op,
        "post_op": post_op,
        "provider_taxonomy_code": provider_taxonomy_code,
        "endo_base": endo_base,
        "service_date": fit_date(date_of_service),
        "anes_base_unit": anes_base_unit,
        "anes_conversion_factor": anes_conversion_factor
    }

    # Perform any needed adjustments
    if len(comments) == 0:
        line_item_payment_details = perform_adjustments(line_item_payment_details, data_item_index, ncci_info)

    return line_item_payment_details


def price_line_item_prepare(db, line_item: dict) -> Tuple[dict, dict, dict, str, float, float]:
    cpt = line_item['code']
    mod1 = line_item['mod1']
    mod2 = line_item['mod2']
    mod3 = line_item['mod3']
    mod4 = line_item['mod4']
    date_of_service = line_item['service_date']

    rendering_provider_npi = line_item["rendering_provider_npi"]

    provider_zip = find_zip_by_npi(db, rendering_provider_npi)
    if not provider_zip:
        raise ValueError(f"Couldn't find zip code for NPI {rendering_provider_npi}")
    region = find_region_by_zip(provider_zip)
    if not region:
        raise ValueError(f"Couldn't find medicare region for zip code {provider_zip}")
    provider_taxonomy_code = find_tc_by_npi(db, rendering_provider_npi)
    if not provider_taxonomy_code:
        raise ValueError(f"Couldn't find taxonomy code for NPI {rendering_provider_npi}")
    gpci_info = get_gpci(db, region['carrier'], region['locality'], date_of_service)

    # Get RVU values for correct cpt/mod
    for mod in [mod1, mod2, mod3, mod4]:
        rvus = get_rvus(db, cpt, mod, date_of_service)
        if rvus is not None:
            break

    anes_base_unit = get_base_unit(db, cpt, date_of_service)
    anes_conversion_factor = get_conversion_factor(db, region['carrier'], region['locality'], date_of_service)

    return line_item, gpci_info, rvus, provider_taxonomy_code, anes_base_unit, anes_conversion_factor


def ncci_info_prepare(db, line_items: List[dict]) -> List[List[dict]]:
    ncci_info = []

    # 1. Check every line_items pair and compare code_1, code_2 with NCCI col_1, col_2
    # 2. If claim has two codes on the same service date that appear as a pair
    for line_item_index, line_item in enumerate(line_items):
        # If line_item second code
        if line_item_index % 2 != 0:
            if line_items[line_item_index - 1]['service_date'] == line_item['service_date']:
                service_date = datetime.strptime(line_item['service_date'], '%m/%d/%Y').strftime('%Y/%m/%d')

                ncci_rows = get_ncci(
                    db, line_items[line_item_index - 1]['code'], line_item['code'], service_date
                )
                ncci_info.append(ncci_rows)
        else:
            ncci_info.append([])

    return ncci_info


def price_claim_get(claim: dict, data: List[Tuple[dict, dict, dict, str, float, float]], ncci_info: List[List[dict]]):
    total_payment = 0.0
    total_charges = 0.0
    line_items = []
    npi = claim['npi']
    claim_number = claim['claim_number']
    service_from = claim['service_from']
    service_to = claim['service_to']

    for data_item_index, data_item in enumerate(data):
        line_items.append(
            price_line_item_get(
                data_item, data_item_index, ncci_info
            )
        )

    perform_adjustments_bilateral_surgery(line_items)
    perform_adjustments_multiple(line_items)
    perform_adjustments_anesthesia_pricing(line_items)

    for line_item in line_items:
        payment = to_currency(line_item["line_item_payment"])
        line_item["line_item_payment"] = payment
        total_payment += payment
        total_charges += line_item["charges"]

    return {
        "claim_number": claim_number,
        "service_from": fit_date(service_from),
        "service_to": fit_date(service_to),
        "npi": npi,
        "total_claim_charges": to_currency(total_charges),
        "total_claim_payment": to_currency(total_payment),
        "line_items": line_items
    }


def price_claim(claim, reference_database_connection=None):
    temporary_database_connection = None

    if reference_database_connection is None:
        # If a database connection wasn't provided, open one
        db_config = get_db_config_from_env("t_data")
        temporary_database_connection = get_db(db_config)
        reference_database_connection = temporary_database_connection

    data = [
        price_line_item_prepare(reference_database_connection, line_item)
        for line_item in claim['line_items']
    ]

    ncci_info = ncci_info_prepare(reference_database_connection, claim['line_items'])

    # Clean up database connection if we opened it
    if temporary_database_connection:
        temporary_database_connection.close()

    return price_claim_get(claim, data, ncci_info)
