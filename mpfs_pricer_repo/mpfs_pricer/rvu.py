def get_rvus(db, cpt, mod, date_of_service):
    if mod == "":
        mod = None

    query = """
            SELECT
                hcpcs, "MOD", description, "STATUS CODE", "WORK RVU", "NON-FAC PE RVU",
                "NON-FAC NA INDICATOR", "FACILITY PE RVU", "FACILITY NA INDICATOR", "MP RVU", "NON-FACILITY TOTAL",
                "FACILITY TOTAL", "PCTC IND", "GLOB DAYS", "PRE OP", "INTRA OP", "POST OP", "MULT PROC", "BILAT SURG",
                "ASST SURG", "CO-SURG", "TEAM SURG", "ENDO BASE", "CONV FACTOR",
                "PHYSICIAN SUPERVISION OF DIAGNOSTIC PROCEDURES", "CALCULATION FLAG",
                "DIAGNOSTIC IMAGING FAMILY INDICATOR", "NON-FACILITY PE USED FOR OPPS PAYMENT AMOUNT",
                "FACILITY PE USED FOR OPPS PAYMENT AMOUNT", "MP USED FOR OPPS PAYMENT AMOUNT"
            FROM internal_reference.cms_pfs_rvu
            where
                hcpcs = %s and
                "MOD" IS NOT DISTINCT FROM %s and
                eff_start_dt <= %s and
                eff_end_dt >= %s

        """

    cursor = db.cursor()
    cursor.execute(query, [cpt, mod, date_of_service, date_of_service])

    res = cursor.fetchone()
    if not res:
        return None

    cpt, mod, desc, status_code, work_rvu, nonfac_pe_rvu, nonfac_na_indicator, fac_pe_rvu, fac_na_indicator, mp_rvu, nonfac_total, fac_total, pctc_ind, glob_days, pre_op, intra_op, post_op, multi_proc, bilat_surg, asst_surg, co_surg, team_surg, endo_base, conv_factor, phys_super, calc_flag, imaging_ind, nonfac_pe_opps, fac_pe_opps, mp_opps = res
    return {
        "cpt": cpt,
        "mod": mod,
        "desc": desc,
        "status_code": status_code,
        "work_rvu": work_rvu,
        "nonfac_pe_rvu": nonfac_pe_rvu,
        "nonfac_na_indicator": nonfac_na_indicator,
        "fac_pe_rvu": fac_pe_rvu,
        "fac_na_indicator": fac_na_indicator,
        "mp_rvu": mp_rvu,
        "nonfac_total": nonfac_total,
        "fac_total": fac_total,
        "pctc_ind": pctc_ind,
        "glob_days": glob_days,
        "pre_op": pre_op,
        "intra_op": intra_op,
        "post_op": post_op,
        "multi_proc": multi_proc,
        "bilat_surg": bilat_surg,
        "asst_surg": asst_surg,
        "co_surg": co_surg,
        "team_surg": team_surg,
        "endo_base": endo_base,
        "conv_factor": conv_factor,
        "phys_super": phys_super,
        "calc_flag": calc_flag,
        "imaging_ind": imaging_ind,
        "nonfac_pe_opps": nonfac_pe_opps,
        "fac_pe_opps": fac_pe_opps,
        "mp_opps": mp_opps,
    }
