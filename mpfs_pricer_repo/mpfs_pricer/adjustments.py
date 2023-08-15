from typing import List, Dict, Iterator, Callable

adjust_physician_assistant_multiplier = {
    0: 0.16 * 0.85,
    1: 0.00,
    2: 0.16 * 0.85,
    9: 1.00
}

adjust_physician_assistant_comment = {
    0: "Assistants at surgery are not paid unless supporting documentation is submitted",
    1: "Assistants at surgery may not be paid",
    2: "Assistants at surgery may be paid",
    9: "Concept does not apply"
}


def adjust_physician_assistant(line_item_payment_details):
    # - Physician Assistant at Surgery: 16% of 85% of MPFS allowed
    #     - Identified with Modifier (AS)
    return {
        "multiplier": adjust_physician_assistant_multiplier[line_item_payment_details["asst_surg"]],
        "comment": adjust_physician_assistant_comment[line_item_payment_details["asst_surg"]]
    }


adjust_assistant_at_surgery_multiplier = {
    0: 0.16,
    1: 0.00,
    2: 0.16,
    9: 1.00
}

adjust_assistant_at_surgery_comment = {
    0: "Assistants at surgery are not paid unless supporting documentation is submitted",
    1: "Assistants at surgery may not be paid",
    2: "Assistants at surgery may be paid",
    9: "Concept does not apply"
}


def adjust_assistant_at_surgery(line_item_payment_details):
    # - Assistant at Surgery Services: Modifiers 80, 81, 82 will be appended to the HCPCS code. Payment is calculated at 16% of the fee schedule allowable
    return {
        "multiplier": adjust_assistant_at_surgery_multiplier[line_item_payment_details["asst_surg"]],
        "comment": adjust_assistant_at_surgery_comment[line_item_payment_details["asst_surg"]]
    }


adjust_co_surgeons_multiplier = {
    0: 0.00,
    1: 0.625,
    2: 0.625,
    9: 1.00
}

adjust_co_surgeons_comment = {
    0: "Co-Surgeons not permitted",
    1: "Co-surgeon may be paid if supporting documentation is submitted",
    2: "Co-surgeons are paid, no documentation is required",
    9: "Concept does not apply"
}


def adjust_co_surgeons(line_item_payment_details):
    # - Modifier 62: Co-surgeons. If payable, 62.5% of MPFS allowable
    return {
        "multiplier": adjust_co_surgeons_multiplier[line_item_payment_details["co_surg"]],
        "comment": adjust_co_surgeons_comment[line_item_payment_details["co_surg"]]
    }


adjust_team_surgeons_multiplier = {
    0: 0.00,
    1: 1.00,
    2: 1.00,
    9: 1.00
}

adjust_team_surgeons_comment = {
    0: "Team surgeons not permitted/ paid",
    1: "Team surgeons may be paid with documentation submission",
    2: "Team surgeons permitted",
    9: "Concept does not apply"
}


def adjust_team_surgeons(line_item_payment_details):
    # - Team Surgery (Modifier 66)
    return {
        "multiplier": adjust_team_surgeons_multiplier[line_item_payment_details["team_surg"]],
        "comment": adjust_team_surgeons_comment[line_item_payment_details["team_surg"]]
    }


def adjust_taxonomy_code_licensed_clinical_social_worker(line_item_payment_details):
    # Licensed Clinical Social Worker:
    # Check for: Identification of this provider can be done through Taxonomy Code
    # Action: Payment is made at 75% of MPFS
    return {
        "multiplier": 0.75,
        "comment": "Licensed Clinical Social Worker: Payment is made at 75% of MPFS"
    }


def adjust_taxonomy_code_nurse_practitioners(line_item_payment_details):
    # Nurse Practitioners and Clinical Nursing Specialist Services
    # Check for: Identification of this provider can be done through Taxonomy Code
    # Action: Payable at 85% of MPFS.
    return {
        "multiplier": 0.85,
        "comment": "Nurse Practitioners and Clinical Nursing Specialist Services: Payable at 85% of MPFS."
    }


def adjust_taxonomy_code_nutrition_and_dietician(line_item_payment_details):
    # Nutrition and Dietician Services
    # Check for: Identification of this provider can be done through Taxonomy Code
    # Action: Payable at 85% of MPFS.
    return {
        "multiplier": 0.85,
        "comment": "Nutrition and Dietician Services: Payable at 85% of MPFS."
    }


def adjust_taxonomy_code_nurse_midwife(line_item_payment_details):
    # Certified Nurse-Midwife
    # Check for: Identification of Nurse Midwives can be done through Taxonomy Code
    # Action: Payment is made at the lesser of 80% of the actual charge or 100% of MPFS
    return {
        "multiplier": 1.00,
        "charges_multiplier": 0.80,
        "comment": "Certified Nurse-Midwife: Payment is made at the lesser of 80% of the actual charge or 100% of MPFS"
    }


def adjust_taxonomy_code_physician_assistant(line_item_payment_details):
    # Physician Assistant Services
    # Extra check: Can only be paid as long as no facility charges are paid in connection with the service.
    # Action: Payment is lesser of 80% of submitted charge or 85% of MPFS
    return {
        "multiplier": 0.85,
        "charges_multiplier": 0.80,
        "comment": "Physician Assistant Services: Payment is lesser of 80% of submitted charge or 85% of MPFS"
    }


def adjust_crna_mod_qx(line_item_payment_details):
    # - Modifier QX: CRNA service under supervision of physician
    #       Action: Payment is 50% of MPFS
    return {
        "multiplier": 0.50,
        "comment": "CRNA service under supervision of physician: Payment is 50% of MPFS"
    }


def adjust_crna_mod_qy(line_item_payment_details):
    # - Modifier QY: CRNA service under supervision of anesthesiologist
    #       Action: Payment is 50% of MPFS
    return {
        "multiplier": 0.50,
        "comment": "CRNA service under supervision of anesthesiologist: Payment is 50% of MPFS"
    }


def adjust_bilateral_surgery_mod_50(line_item_payment_details):
    return {
        "multiplier": 1.50 if line_item_payment_details["bilat_surg"] == 1 else 1.00,
        "comment": "Bilateral surgery: Final payment is the lower of the total submitted charge or 150% of the fee schedule amount for a single code."
    }


def adjust_bilateral_surgery_2_units(line_item_payment_details):
    return {
        "multiplier": 1.50 / 2 if line_item_payment_details["bilat_surg"] == 1 else 1.00,
        "comment": "Bilateral surgery: Final payment is the lower of the total submitted charge or 150% of the fee schedule amount for a single code."
    }


def adjust_surgical_care_only(line_item_payment_details):
    # - Modifier 54: Surgical care only
    #       Action: Multiply the MPFS allowable by the sum of pre- and intra-operative percentages
    return {
        "multiplier": line_item_payment_details["pre_op"] + line_item_payment_details["intra_op"],
        "comment": "Surgical care only: Multiply the MPFS allowable by the sum of pre- and intra-operative percentages"
    }


def adjust_post_op_care_only(line_item_payment_details):
    # - Modifier 55: Post op care only
    #       Action: Multiply MPFS allowable by post-operative percentage (from RVU table) divided by 90. Multiply result by number of days provider provided post-op care
    return {
        "multiplier": line_item_payment_details["post_op"] / 90,
        "comment": "Post op care only: Multiply MPFS allowable by post-operative percentage (from RVU table) divided by 90. Multiply result by number of days provider provided post-op care"
    }


def find_adjustments_taxonomy_codes(line_item_payment_details):
    adjustments = []

    if line_item_payment_details["provider_taxonomy_code"] in ['175M00000X', '176B00000X', '367A00000X']:
        # - Certified Nurse-Midwife: Payment is made at the lesser of 80% of the actual charge or 100% of MPFS
        #     - Identification of Nurse Midwives can be done through Taxonomy Code
        adjustments.append(adjust_taxonomy_code_nurse_midwife(line_item_payment_details))

    if line_item_payment_details["provider_taxonomy_code"] in ['104100000X', '1041C0700X', '1041S0200X']:
        # - Licensed Clinical Social Worker: Allowed at 75% of MPFS
        #     - Identification of this provider type can be done through Taxonomy Code
        adjustments.append(adjust_taxonomy_code_licensed_clinical_social_worker(line_item_payment_details))

    if line_item_payment_details["provider_taxonomy_code"] in [
        '363L00000X', '363LA2100X', '363LA2200X', '363LC1500X', '363LC0200X', '363LF0000X', '363LG0600X', '363LN0000X',
        '363LN0005X', '363LX0001X', '363LX0106X', '363LP0200X', '363LP0222X', '363LP1700X', '363LP2300X', '363LP0808X',
        '363LS0200X', '363LW0102X',
        '364S00000X', '364SA2100X', '364SA2200X', '364SC2300X', '364SC1501X', '364SC0200X', '364SE0003X', '364SE1400X',
        '364SF0001X', '364SG0600X', '364SH1100X', '364SH0200X', '364SI0800X', '364SL0600X', '364SM0705X', '364SN0000X',
        '364SN0800X', '364SX0106X', '364SX0200X', '364SX0204X', '364SP0200X', '364SP1700X', '364SP2800X', '364SP0808X',
        '364SP0809X', '364SP0807X', '364SP0810X', '364SP0811X', '364SP0812X', '364SP0813X', '364SR0400X', '364SS0200X',
        '364ST0500X', '364SW0102X',
        '367500000X'
    ]:
        # - Nurse Practitioners and Clinical Nursing Specialist Services: Payable at 85% of MPFS.
        #     - Identification of this provider type can be done through Taxonomy Code
        adjustments.append(adjust_taxonomy_code_nurse_practitioners(line_item_payment_details))

    if line_item_payment_details["provider_taxonomy_code"] in [
        '133N00000X', '133NN1002X',
        '133V00000X', '133VN1101X', '133VN1006X', '133VN1201X', '133VN1301X', '133VN1004X', '133VN1401X', '133VN1005X',
        '133VN1501X'
    ]:
        # - Nutrition and Dietician Services: Payable at 85% of MPFS.
        #     - Identification of this provider type can be done through Taxonomy Code
        adjustments.append(adjust_taxonomy_code_nutrition_and_dietician(line_item_payment_details))

    if line_item_payment_details["provider_taxonomy_code"] in ['363A00000X', '363AM0700X', '363AS0400X']:
        # - Physician Assistant Services: Reimbursement can occur in all POS settings as long as no facility charges are paid in connection with the service. Payment is lesser of 80% of submitted charge or 85% of MPFS
        adjustments.append(adjust_taxonomy_code_physician_assistant(line_item_payment_details))

    return adjustments


def find_adjustments_crna(line_item_payment_details, mods):
    adjustments = []

    if "QX" in mods:
        # - Modifier QX: CRNA service under supervision of physician. 50% of MPFS allowable
        adjustments.append(adjust_crna_mod_qx(line_item_payment_details))

    if "QY" in mods:
        # - Modifier QY: CRNA service under supervision of anesthesiologist
        adjustments.append(adjust_crna_mod_qy(line_item_payment_details))

    return adjustments


def find_adjustments_bilateral_surgery(line_item_payment_details, mods):
    adjustments = []

    if "50" in mods:
        # - Modifier 50: Bilateral surgery
        adjustments.append(adjust_bilateral_surgery_mod_50(line_item_payment_details))

    if line_item_payment_details["quantity"] == 2:
        # - 2 units: Bilateral surgery
        adjustments.append(adjust_bilateral_surgery_2_units(line_item_payment_details))

    return adjustments


def perform_adjustments(line_item_payment_details: dict, data_item_index: int, ncci_info: List[List[dict]]):
    adjustments = []
    mods = [
        line_item_payment_details["mod1"],
        line_item_payment_details["mod2"],
        line_item_payment_details["mod3"],
        line_item_payment_details["mod4"],
    ]

    if "AS" in mods:
        # - Physician Assistant at Surgery: 16% of 85% of MPFS allowed
        #     - Identified with Modifier (AS)
        adjustments.append(adjust_physician_assistant(line_item_payment_details))

    if "80" in mods or "81" in mods or "82" in mods:
        # - Assistant at Surgery Services: Modifiers 80, 81, 82 will be appended to the HCPCS code. Payment is calculated at 16% of the fee schedule allowable
        adjustments.append(adjust_assistant_at_surgery(line_item_payment_details))

    # - Diagnostic Imaging PC/TC: Reimburse full amount for the highest paying PC/TC services
    #     - Reimburse 75% for each additional PC (modifier 26) service for same date of service
    #     - Reimburse 50% for each additional TC service for same date of service

    # - Diagnostic Ophthalmology Services: Total amount is based on the TC of the procedure
    #     - Highest technical component service is paid at 100%
    #     - Subsequent services are paid at 80%

    # - Multiple Endoscopies: See Multiple Procedure Indicator “3”

    [adjustments.append(adjustment) for adjustment in find_adjustments_taxonomy_codes(line_item_payment_details)]

    # - Modifier 26: Professional Component (PC) only, reduction is made through within the MPFS

    # - Modifier 52, 53: Partially reduced and discontinued services (respectively), charge amount should reflect the % reduction in services provided. Medicare’s claim processing system will pay the lower of the submitted charge or the allowed amount per the fee schedule

    if "54" in mods:
        # - Modifier 54: Surgical care only. Multiply the MPFS allowable by the sum of pre- and intra-operative percentages
        adjustments.append(adjust_surgical_care_only(line_item_payment_details))

    if "55" in mods:
        # - Modifier 55: Post op care only. Multiply MPFS allowable by post-operative percentage divided by 90. Multiply result by number of days provider provided post=op care
        adjustments.append(adjust_post_op_care_only(line_item_payment_details))

    if "62" in mods:
        # - Modifier 62: Co-surgeons. 62.5% of MPFS allowable
        adjustments.append(adjust_co_surgeons(line_item_payment_details))

    if "66" in mods:
        # - Modifier 66: team surgeons. Priced by report
        adjustments.append(adjust_team_surgeons(line_item_payment_details))

    # - Modifier 78: return to the operating room for related procedure

    # - Modifier TC: Technical component only

    # - Multiple Procedure Payment Reduction: HCPCS designated as “therapy only” have a reduced practice expense portion of the allowable

    # - NCCI Edits
    ncci_adjustments(line_item_payment_details, data_item_index, ncci_info)

    [adjustments.append(adjustment) for adjustment in find_adjustments_crna(line_item_payment_details, mods)]

    [adjustments.append(adjustment) for adjustment in
     find_adjustments_bilateral_surgery(line_item_payment_details, mods)]

    charges = float(line_item_payment_details["charges"])
    # update with minimal adjustment multiplier
    if len(adjustments) > 0:
        adjustment = min(adjustments,
                         key=lambda a: min(a["multiplier"] * line_item_payment_details["line_item_payment"],
                                           a.get("charges_multiplier", 1.00) * charges))
        if adjustment:
            line_item_payment_details["line_item_payment"] = min(
                adjustment["multiplier"] * line_item_payment_details["line_item_payment"],
                adjustment.get("charges_multiplier", 1.00) * charges)
            line_item_payment_details["comments"].append(adjustment["comment"])

    # compare calculated price with actual charges
    if line_item_payment_details["line_item_payment"] > charges:
        line_item_payment_details["line_item_payment"] = charges

    return line_item_payment_details


def group_by_key(key_field: str, line_item_list: Iterator[dict]) -> Dict[str, List[dict]]:
    result = {}
    for line_item in line_item_list:
        key = line_item[key_field]
        data = result.get(key)
        if not data:
            result[key] = [line_item]
        else:
            data.append(line_item)
    for key in result:
        result[key] = sorted(result[key], key=lambda item: item["line_item_payment"], reverse=True)
    return result


def group_by_date(multi_proc: int, line_item_list: List[dict], func: Callable[[dict], bool] = lambda item: True) -> \
        Dict[str, List[dict]]:
    return group_by_key("service_date",
                        filter(lambda item: item["multi_proc"] == multi_proc and func(item), line_item_list))


def perform_adjustments_multiple_2(line_item_list: List[dict]):
    for line_items in group_by_key("service_date",
                                   filter(lambda item: item["multi_proc"] in [2, 3], line_item_list)).values():
        data_2 = []
        data_3 = []
        for line_item in line_items:
            if line_item["multi_proc"] == 2:
                data_2.append([line_item])
            else:
                data_3.append(line_item)
        data = sorted(data_2 + [*group_by_key("endo_base", data_3).values()],
                      key=lambda items: sum(row["line_item_payment"] for row in items), reverse=True)
        if len(data) > 1:
            for item_row in data[1]:
                item_row["line_item_payment"] *= 0.5
            if len(data) > 2:
                for item_rows in data[2:]:
                    for item_row in item_rows:
                        item_row["line_item_payment"] *= 0.25


def perform_adjustments_multiple_3(line_item_list: List[dict]):
    for line_items in group_by_date(3, line_item_list).values():
        for grouped_line_items in group_by_key("endo_base", line_items).values():
            if len(grouped_line_items) > 1:
                grouped_line_items[1]["line_item_payment"] *= 0.5
                if len(grouped_line_items) > 2:
                    for line_item in grouped_line_items[2:]:
                        line_item["line_item_payment"] *= 0.25


def perform_adjustments_multiple_4(line_item_list: List[dict]):
    for line_items in group_by_date(4, line_item_list, lambda item: "TC" in [item["mod1"], item["mod2"], item["mod3"],
                                                                             item["mod4"]]).values():
        if len(line_items) > 1:
            for line_item in line_items[1:]:
                line_item["line_item_payment"] *= 0.5


def perform_adjustments_multiple_5(line_item_list: List[dict]):
    for line_items in group_by_date(5, line_item_list).values():
        for line_item in line_items:
            line_item["line_item_payment"] *= 0.5


def perform_adjustments_multiple_6(line_item_list: List[dict]):
    for line_items in group_by_date(6, line_item_list).values():
        if len(line_items) > 1:
            for line_item in line_items[1:]:
                line_item["line_item_payment"] *= 0.75


def perform_adjustments_multiple_7(line_item_list: List[dict]):
    for line_items in group_by_date(7, line_item_list, lambda item: "TC" in [item["mod1"], item["mod2"], item["mod3"],
                                                                             item["mod4"]]).values():
        if len(line_items) > 1:
            for line_item in line_items[1:]:
                line_item["line_item_payment"] *= 0.8


def perform_adjustments_multiple(line_item_list: List[dict]):
    if len(line_item_list) < 2:
        return
    # Run adjustment for multi_proc=3 before multi_proc=2
    perform_adjustments_multiple_3(line_item_list)
    # Run adjustment for multi_proc=2 including groups of multi_proc=3
    perform_adjustments_multiple_2(line_item_list)
    # Run adjustment for other multi_proc values
    perform_adjustments_multiple_4(line_item_list)
    perform_adjustments_multiple_5(line_item_list)
    perform_adjustments_multiple_6(line_item_list)
    perform_adjustments_multiple_7(line_item_list)


def group_by_date_cpt(line_item_list: List[dict]) -> Dict[str, List[dict]]:
    result = {}
    for line_item in line_item_list:
        mods = [
            line_item["mod1"],
            line_item["mod2"],
            line_item["mod3"],
            line_item["mod4"],
        ]
        if line_item["bilat_surg"] == 1 and ("LT" in mods or "RT" in mods):
            key = line_item["service_date"] + "_" + line_item["code"]
            data = result.get(key)
            if not data:
                result[key] = [line_item]
            else:
                data.append(line_item)
    for key in result:
        result[key] = result[key] if len(
            list(
                filter(lambda item: "LT" in [item["mod1"], item["mod2"], item["mod2"], item["mod2"]],
                       result[key]))) == 1 and len(
            list(
                filter(lambda item: "RT" in [item["mod1"], item["mod2"], item["mod2"], item["mod2"]],
                       result[key]))) == 1 else []
    return result


def perform_adjustments_bilateral_surgery(line_item_list: List[dict]):
    for line_items in group_by_date_cpt(line_item_list).values():
        if len(line_items) > 1:
            for line_item in line_items:
                line_item["line_item_payment"] *= 0.75
    return


def perform_adjustments_anesthesia_pricing(line_item_list: List[dict]) -> None:
    for line_items in group_by_key("service_date",
                                   filter(lambda item: "00100" <= item["code"] <= "01999", line_item_list)).values():
        max_anes_base_unit = 0
        sum_units = 0

        line_items_AD_count = len(
            list(
                filter(
                    lambda item: 'AD' in [item['mod1'], item['mod2'], item['mod3'], item['mod4']],
                    line_items
                )
            )
        )

        for line_item in line_items:
            # If modifier = AD and more than 4 procedures are performed set Base Units = 3
            anes_base_unit = 3 if line_items_AD_count > 4 else line_item["anes_base_unit"]

            max_anes_base_unit = max(max_anes_base_unit, anes_base_unit)
            sum_units += line_item["quantity"]

        for line_item in line_items:
            line_item["line_item_payment"] = (max_anes_base_unit + sum_units / 15) * line_item["anes_conversion_factor"]

            mods = [line_item['mod1'], line_item['mod2'], line_item['mod3'], line_item['mod4']]

            if any(x in mods for x in ['QZ', 'AA']):
                continue

            # Reduce payment by 50% if modifiers QX, QS, QK and QY
            if any(x in mods for x in ['QX', 'QS', 'QK', 'QY']):
                line_item["line_item_payment"] /= 2


def ncci_adjustments(line_item_payment_details: dict, data_item_index: int, ncci_info: List[List[dict]]):
    mods = [
        line_item_payment_details['mod1'],
        line_item_payment_details['mod2'],
        line_item_payment_details['mod3'],
        line_item_payment_details['mod4'],
    ]

    for ncci_rows_index, ncci_rows in enumerate(ncci_info):
        if ncci_rows_index != data_item_index:
            continue

        for ncci_row in ncci_rows:
            # if Modifier Allowed value = 0 or value = 9
            if int(ncci_row['modifier']) in [0, 9]:
                # set payment for Column2 code to $0
                line_item_payment_details['line_item_payment'] = 0
                return

            # if Modifier Allowed value = 1, check Column2 code for Modifiers not in 59,XE,XS,XU or XP
            elif int(ncci_row['modifier']) == 1 and not any(x in mods for x in ['59', 'XE', 'XS', 'XU', 'XP']):
                # set payment for Column2 code to $0
                line_item_payment_details['line_item_payment'] = 0
                return
