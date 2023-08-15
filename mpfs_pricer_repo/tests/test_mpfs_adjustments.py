import unittest
import itertools
from datetime import date
from typing import Callable, Union, List, Dict, Tuple
from datetime import datetime
from mpfs_pricer import pricer, database, utils
from test_mpfs_adjustments_data import rvus_list, gpci_info, provider_taxonomy_codes, ncci_data, anesthesia_base_unit_list, \
    anesthesia_conversation_factor_list

CHARGES_COEF = 1.5
PRE_CALCULATION = False  # set it True to make pre calculation for test results
DB_ENABLED = False  # set it True to get prices from db for test results


# TEST DATA format for test cases:
# list of:
#   <base price per 1 unit/quantity>: float,
#   <additional properties, e.g. "code", "quantity", mods>: dict,
#   <expected price coefficient>: float,
#   <extra properties, e.g. "carrier", "locality_code">: dict


class AdjustmentsTestCase(unittest.TestCase):
    db = None

    @classmethod
    def setUpClass(cls):
        # Allow the reference database connection to be reused between tests since it is read-only
        # and will make the test run more quickly.
        if DB_ENABLED:
            db_config = database.get_db_config_from_env("t_data")
            cls.db = database.get_db(db_config)

    @classmethod
    def tearDownClass(cls):
        if DB_ENABLED:
            cls.db.close()

    @staticmethod
    def setUpLineItem(**kwargs) -> Dict[str, Union[str, int, float]]:
        return {
            'service_date': f'{kwargs.get("service_date", "09/01/2020")}',
            'place_of_service': f'{kwargs.get("place_of_service", "11")}',
            'code': f'{kwargs.get("code", 15757)}',
            'mod1': f'{kwargs.get("mod1", "")}',
            'mod2': f'{kwargs.get("mod2", "")}',
            'mod3': f'{kwargs.get("mod3", "")}',
            'mod4': f'{kwargs.get("mod4", "")}',
            'charges': f'{float(kwargs.get("charges", 100.00)) * int(kwargs.get("quantity", 1))}',
            'quantity': f'{kwargs.get("quantity", 1)}',
            'rendering_provider_npi': f'{kwargs.get("rendering_provider_npi", "1659327898")}',
        }

    @staticmethod
    def setUpData(*args, **kwargs) -> Dict[str, Union[str, float, dict]]:
        return {
            'claim_number': f'{kwargs.get("claim_number", "TEST")}',
            'charges': str(sum(
                float(line_item.get("charges", 100.00)) * int(line_item.get("quantity", 1)) for line_item in args
            )),
            'npi': f'{kwargs.get("npi", "1073640454")}',
            'service_from': str(min(
                datetime.strptime(line_item.get("service_date", "09/01/2020"), "%m/%d/%Y") for line_item in args
            ).strftime("%m/%d/%Y")),
            'service_to': str(max(
                datetime.strptime(line_item.get("service_date", "09/01/2020"), "%m/%d/%Y") for line_item in args
            ).strftime("%m/%d/%Y")),
            'line_items': [AdjustmentsTestCase.setUpLineItem(**line_item) for line_item in args]
        }

    @staticmethod
    def get_gpci_info(carrier: int, locality_code):
        for item in gpci_info:
            if item["carrier"] == carrier and (locality_code is None or locality_code == item["locality_code"]):
                return item

    @staticmethod
    def get_rvus(line_item: dict) -> dict:
        for mod in [line_item['mod1'], line_item['mod2'], line_item['mod3'], line_item['mod4']]:
            for item in rvus_list:
                if item["cpt"] == line_item["code"] and item["mod"] == mod:
                    return item

        for item in rvus_list:
            if item["cpt"] == line_item["code"] and item["mod"] is None:
                return item

        raise Exception(f"No rvu with {line_item['code']} code in the mocks")

    @staticmethod
    def get_ncci(
            line_items: List[Dict[str, Union[str, float, dict]]]
    ) -> List[List[dict]]:
        ncci_info = []

        # 1. Check every line_items pair and compare code_1, code_2 with NCCI col_1, col_2
        # 2. If claim has two codes on the same service date that appear as a pair
        for line_item_index, line_item in enumerate(line_items):
            # If line_item second code
            if line_item_index % 2 != 0:
                if line_items[line_item_index - 1]['service_date'] == line_item['service_date']:
                    ncci_rows = []

                    for ncci_row in ncci_data:
                        if ncci_row['col_1'] == line_items[line_item_index - 1]['code'] \
                                and ncci_row['col_2'] == line_item['code'] \
                                and utils.is_date_between_dates(
                            datetime.strptime(line_item['service_date'], '%m/%d/%Y').date(),
                            ncci_row['effective_date'],
                            ncci_row['deletion_date'],
                        ):
                            ncci_rows.append(ncci_row)

                    ncci_info.append(ncci_rows)
            else:
                ncci_info.append([])

        return ncci_info

    @staticmethod
    def get_taxonomy_code(line_item: dict):
        for item in provider_taxonomy_codes:
            if item['npi'] == line_item['rendering_provider_npi']:
                return item['taxonomy_code']

    @staticmethod
    def get_anes_base_unit(line_item: dict) -> Union[float, None]:
        service_date = datetime.strptime(line_item['service_date'], '%m/%d/%Y')

        for base_unit in anesthesia_base_unit_list:
            beg_eff_date = datetime.strptime(base_unit['beg_eff_date'], '%Y-%m-%d')
            end_eff_date = datetime.strptime(base_unit['end_eff_date'], '%Y-%m-%d')

            if line_item['code'] == base_unit['code'] \
                    and beg_eff_date <= service_date <= end_eff_date:
                return base_unit['base_unit']

    @staticmethod
    def get_anes_conversion_factor(line_item: dict, carrier: int, locality_code) -> Union[float, None]:
        service_date = datetime.strptime(line_item['service_date'], '%m/%d/%Y')

        for conversation_factor in anesthesia_conversation_factor_list:
            beg_eff_date = datetime.strptime(conversation_factor['beg_eff_date'], '%Y-%m-%d')
            end_eff_date = datetime.strptime(conversation_factor['end_eff_date'], '%Y-%m-%d')

            if str(carrier) == conversation_factor['contractor'] \
                    and str(locality_code) == conversation_factor['locality'] \
                    and beg_eff_date <= service_date <= end_eff_date:
                return conversation_factor['Conversion_Factor']

    @staticmethod
    def data_get(line_item: dict, carrier: int, locality_code) -> \
            Tuple[dict, dict, dict, str, Union[float, None], Union[float, None]]:
        return (
            line_item,
            AdjustmentsTestCase.get_gpci_info(carrier, locality_code),
            AdjustmentsTestCase.get_rvus(line_item),
            AdjustmentsTestCase.get_taxonomy_code(line_item),
            AdjustmentsTestCase.get_anes_base_unit(line_item),
            AdjustmentsTestCase.get_anes_conversion_factor(line_item, carrier, locality_code)
        )

    @staticmethod
    def price_get(line_item: dict, carrier: int, locality_code) -> float:
        return pricer.price_line_item_get(
            AdjustmentsTestCase.data_get(line_item, carrier, locality_code)
        )["line_item_payment"]

    @staticmethod
    def extra_data_parse(extra_data):
        return (
            extra_data[0].get("carrier", 12402),
            extra_data[0].get("charges_multiplier", 1.00),
            extra_data[0].get("locality_code", None),
        ) if extra_data else (12402, 1.0, None,)

    def base_test_adjustment_db(self, expected_price: float, line_items: List[dict]):
        if not DB_ENABLED:
            return
        claim = self.setUpData(*line_items)
        result = pricer.price_claim(claim, self.db)
        total_payment = result["total_claim_payment"]
        self.assertAlmostEqual(total_payment, expected_price, msg="[DB] Price validation", places=2)

    def compare_price_with_db(self, expected_price: float, line_data: dict, charges: float):
        if line_data.get("quantity", 1) > 1 or line_data.get("rendering_provider_npi", None):
            return
        has_tc = False
        has_26 = False
        line_data_copy = {**line_data}.copy()
        mods = [line_data_copy.get('mod1', ''), line_data_copy.get('mod2', ''), line_data_copy.get('mod3', ''),
                line_data_copy.get('mod4', '')]
        if 'TC' in mods:
            has_tc = True
        elif '26' in mods:
            has_26 = True
        line_data_copy.pop('mod1', None)
        line_data_copy.pop('mod2', None)
        line_data_copy.pop('mod3', None)
        line_data_copy.pop('mod4', None)
        self.base_test_adjustment_db(expected_price, [
            {**line_data_copy, 'charges': charges, 'mod1': 'TC' if has_tc else '26' if has_26 else ''}])

    def base_test_adjustment_price(self, data: list, ignore_db: bool = False):
        for price, line_data, multiplier, *extra_data in data:
            carrier, charges_multiplier, locality_code = self.extra_data_parse(extra_data)
            # Tests cases when charges in claim are bigger than calculated price
            charges = price * CHARGES_COEF
            expected_price = price * multiplier
            with self.subTest(price=price, line_data=line_data, charges="Charges bigger than calculated price"):
                if not ignore_db:
                    self.compare_price_with_db(price, line_data, charges)
                    self.base_test_adjustment_db(expected_price, [{**line_data, 'charges': charges}])
                line_item = self.setUpLineItem(charges=charges, **line_data)
                self.assertAlmostEqual(
                    self.price_get(line_item, carrier, locality_code), expected_price, places=2
                )
            # Tests cases when charges in claim are less than calculated price
            units = line_data.get("quantity", 1)
            charges = max(price * multiplier / units - 1, 0)
            expected_price = charges * units * charges_multiplier
            with self.subTest(price=price, line_data=line_data, charges="Charges less than calculated price"):
                if not ignore_db:
                    self.base_test_adjustment_db(expected_price, [{**line_data, 'charges': charges}])
                line_item = self.setUpLineItem(charges=charges, **line_data)
                self.assertAlmostEqual(
                    self.price_get(line_item, carrier, locality_code), expected_price, places=2
                )
            # Tests only when price is not 0.00 to make sure that test are valid for all cases
            # Tests with price 0.00 will always pass and will not show bug in the calculations
            if multiplier != 0.00:
                with self.subTest(price=price, line_data=line_data, charges="Avoid 0 calculated price"):
                    line_item = self.setUpLineItem(charges=price * CHARGES_COEF, **line_data)
                    self.assertNotEqual(self.price_get(line_item, carrier, locality_code), 0.00)

    def base_test_multiple_procedure_adjustment_price(self, data: List[list], expected_price: float,
                                                      ignore_db: bool = False):
        line_items = []
        items = []

        for data_item in itertools.chain(*data):
            price = data_item[0]
            line_data = data_item[1]
            extra_data = [] if len(data_item) <= 2 else [data_item[2]]

            carrier, charges_multiplier, locality_code = self.extra_data_parse(extra_data)
            charges = price * CHARGES_COEF
            if not ignore_db:
                self.compare_price_with_db(price, line_data, charges)
            line_item = self.setUpLineItem(charges=charges, **line_data)
            items.append(
                self.data_get(line_item, carrier, locality_code)
            )
            line_items.append(line_item)
        if not ignore_db:
            self.base_test_adjustment_db(expected_price, line_items)
        claim = self.setUpData(*line_items)
        result = pricer.price_claim_get(claim, items, AdjustmentsTestCase.get_ncci(line_items))
        total_payment = result["total_claim_payment"]
        self.assertAlmostEqual(total_payment, expected_price, places=2)

    def base_test_multiple_procedure_adjustment_price_pre_calc(self, data: List[list], func: Callable[[list], float]):
        if not PRE_CALCULATION:
            return
        total_price = 0.00
        for line_items in data:
            prices = []
            for price, line_data in line_items:
                carrier, charges_multiplier, locality_code = self.extra_data_parse([])
                calculated_price = self.price_get(
                    self.setUpLineItem(charges=price * CHARGES_COEF, **line_data),
                    carrier,
                    locality_code,
                )
                prices.append(calculated_price)
                with self.subTest(price=price, line_data=line_data, charges="Avoid 0 calculated price"):
                    self.assertNotEqual(calculated_price, 0.00)
            total_price += func(sorted(prices, reverse=True))
        self.base_test_multiple_procedure_adjustment_price(data, total_price)

    # Base price, No adjustments
    def test_price_basic_surgery(self):
        data = [
            (1409.2613468159996, {"code": 27254}, 1.0),
            (1147.533071488, {'code': 29807}, 1.0),
            (279.544989056, {'code': 70482}, 1.0),
            (449.88934463999993, {'code': 71550}, 1.0),
            (348.49922239999995, {'code': 70336}, 1.0),
            (88.66709465599999, {'code': 76604}, 1.0),
            (211.48866495999997, {'code': 70482, 'mod1': 'TC'}, 1.0),
            (370.81703104, {'code': 71550, 'mod1': 'TC'}, 1.0),
            (268.68346303999994, {'code': 70336, 'mod1': 'TC'}, 1.0),
            (57.537288383999986, {'code': 76604, 'mod1': 'TC'}, 1.0),
            (13.242717823999998, {'code': 76514}, 1.0),
            (41.067077632, {'code': 92025}, 1.0),
            (70.45556070399999, {'code': 92060}, 1.0),
            (53.956117375999995, {'code': 76516}, 1.0),
            (4.427833023999999, {'code': 76514, 'mod1': 'TC'}, 1.0),
            (19.543601087999996, {'code': 92025, 'mod1': 'TC'}, 1.0),
            (29.348423615999994, {'code': 92060, 'mod1': 'TC'}, 1.0),
            (28.939889343999994, {'code': 76516, 'mod1': 'TC'}, 1.0),
            (35.116985279999994, {'code': 20979}, 1.0),
            (636.556665408, {'code': 33257}, 1.0),
            (541.77346624, {'code': 78452}, 1.0),
            (25.016228032, {'code': 76516, 'mod1': '26'}, 1.0),
            (1079.3612606719998, {'code': 37216}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    # - Physician Assistant at Surgery: 16% of 85% of MPFS allowed
    #     - Identified with Modifier (AS)
    def test_physicial_assistant_at_surgery_mod_as_rvu_0(self):
        data = [
            (1285.882192192, {"code": 21249, "mod1": 'AS'}, 0.16 * 0.85)
        ]
        self.base_test_adjustment_price(data)

    def test_physicial_assistant_at_surgery_mod_as_rvu_1(self):
        data = [
            (400.05682496, {"code": 28020, "mod1": 'AS'}, 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_physicial_assistant_at_surgery_mod_as_rvu_2(self):
        data = [
            (1409.2613468159996, {"code": 27254, "mod1": 'AS'}, 0.16 * 0.85)
        ]
        self.base_test_adjustment_price(data)

    def test_physicial_assistant_at_surgery_mod_as_rvu_9(self):
        data = [
            (361.01762195199996, {"code": 22526, "mod1": 'AS'}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    # - Assistant at Surgery Services: Modifiers 80 will be appended to the HCPCS code. Payment is calculated at 16% of
    # the fee schedule allowable
    def test_assistant_at_surgery_mod_80_rvu_0(self):
        data = [
            (1285.882192192, {"code": 21249, "mod1": 80}, 0.16)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_80_rvu_1(self):
        data = [
            (400.05682496, {"code": 28020, "mod1": 80}, 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_80_rvu_2(self):
        data = [
            (1409.2613468159996, {"code": 27254, "mod1": 80}, 0.16)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_80_rvu_9(self):
        data = [
            (361.01762195199996, {"code": 22526, "mod1": 80}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_81_rvu_0(self):
        data = [
            (1285.882192192, {"code": 21249, "mod1": 81}, 0.16)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_81_rvu_1(self):
        data = [
            (400.05682496, {"code": 28020, "mod1": 81}, 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_81_rvu_2(self):
        data = [
            (1409.2613468159996, {"code": 27254, "mod1": 81}, 0.16)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_81_rvu_9(self):
        data = [
            (361.01762195199996, {"code": 22526, "mod1": 81}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_82_rvu_0(self):
        data = [
            (1285.882192192, {"code": 21249, "mod1": 82}, 0.16)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_82_rvu_1(self):
        data = [
            (400.05682496, {"code": 28020, "mod1": 82}, 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_82_rvu_2(self):
        data = [
            (1409.2613468159996, {"code": 27254, "mod1": 82}, 0.16)
        ]
        self.base_test_adjustment_price(data)

    def test_assistant_at_surgery_mod_82_rvu_9(self):
        data = [
            (361.01762195199996, {"code": 22526, "mod1": 82}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    # - Certified Nurse-Midwife: Payment is made at the lesser of 80% of the actual charge or 100% of MPFS
    #     - Identification of Nurse Midwives can be done through Taxonomy Code
    def test_certified_nurse_midwife(self):
        data = [
            (2189.30701376, {"rendering_provider_npi": '1003003773'}, 1.0,
             {"carrier": 6302, "charges_multiplier": 0.80}),
            (2403.4995115519996, {"rendering_provider_npi": '1003016932'}, 1.0,
             {"carrier": 9102, "locality_code": 99, "charges_multiplier": 0.80}),
            (
                2189.30701376, {"rendering_provider_npi": '1003009010'}, 1.0,
                {"carrier": 6302, "charges_multiplier": 0.80})
        ]
        self.base_test_adjustment_price(data)

    # - Diagnostic Imaging PC/TC: Reimburse full amount for the highest paying PC/TC services
    #     - Reimburse 75% for each additional PC (modifier 26) service for same date of service
    #     - Reimburse 50% for each additional TC service for same date of service
    # see test_multiple_procedure_adjustments_4

    # - Diagnostic Ophthalmology Services: Total amount is based on the TC of the procedure
    #     - Highest technical component service is paid at 100%
    #     - Subsequent services are paid at 80%
    # see see test_multiple_procedure_adjustments_7

    # - Multiple Endoscopies: See Multiple Procedure Indicator “3”
    # see test_multiple_procedure_adjustments_3

    # - Licensed Clinical Social Worker: Allowed at 75% of MPFS
    #     - Identification of this provider type can be done through Taxonomy Code
    def test_licensed_clinical_social_worker(self):
        data = [
            (2333.9927464319994, {"rendering_provider_npi": '1992999296'}, 0.75, {"carrier": 4212}),
            (2248.3889370239995, {"rendering_provider_npi": '1922001973'}, 0.75, {"carrier": 6202}),
            (2521.0635489279994, {"rendering_provider_npi": '1992990907'}, 0.75, {"carrier": 1182, "locality_code": 18})
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 26: Professional Component (PC) only, reduction is made through within the MPFS
    def test_professional_componet_mod_26(self):
        data = [
            (2507.128993472, {"code": 15757, "mod1": 26}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 52, 53: Partially reduced and discontinued services (respectively), charge amount should reflect
    # the % reduction in services provided. Medicare’s claim processing system will pay the lower of the submitted
    # charge or the allowed amount per the fee schedule
    # - 52,53: there is no set % reduction. We should calculate the final price based on the lower of the standard
    # calculation or the submitted charge
    def test_partially_reduced_mod_52(self):
        data = [
            (2507.128993472, {"code": 15757, "mod1": 52}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    def test_partially_reduced_mod_53(self):
        data = [
            (2507.128993472, {"code": 15757, "mod1": 52}, 1.0)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 54: Surgical care only. Multiply the MPFS allowable by the sum of pre- and intra-operative percentages
    def test_surgical_care_only_mod_54_no_pre_no_intra(self):
        data = [
            (70.33790860799998, {"code": 11981, "mod1": 54}, 0.00 + 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_surgical_care_only_mod_54_pre(self):
        data = [
            (58.56151123199999, {"code": '10040A', "mod1": 54}, 0.20 + 0.00)
        ]
        # No DB data for such combination
        self.base_test_adjustment_price(data, True)

    def test_surgical_care_only_mod_54_intra(self):
        data = [
            (58.56151123199999, {"code": '10040B', "mod1": 54}, 0.00 + 0.70)
        ]
        # No DB data for such combination
        self.base_test_adjustment_price(data, True)

    def test_surgical_care_only_mod_54_pre_intra(self):
        data = [
            (58.56151123199999, {"code": 10040, "mod1": 54}, 0.10 + 0.80)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 55: Post op care only. Multiply MPFS allowable by post-operative percentage divided by 90. Multiply
    # result by number of days provider provided post=op care
    def test_post_op_care_only_mod_55_no_post(self):
        data = [
            (70.33790860799998, {"code": 11981, "mod1": 55}, 0.00 / 90),
            (70.33790860799998, {"code": 11981, "mod1": 55, "quantity": 2}, 0.00 / 90 * 2)
        ]
        self.base_test_adjustment_price(data)

    def test_post_op_care_only_mod_55_with_post(self):
        data = [
            (114.75951455999997, {"code": 10120, "mod1": 55}, 0.10 / 90),
            (114.75951455999997, {"code": 10120, "mod1": 55, "quantity": 2}, 0.10 / 90 * 2)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 62: Co-surgeons. 62.5% of MPFS allowable
    def test_co_surgeons_mod_62_0(self):
        data = [
            (694.9998027519999, {"code": 15920, "mod1": 62}, 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_co_surgeons_mod_62_1(self):
        data = [
            (1022.520501568, {"code": 15832, "mod1": 62}, 0.625)
        ]
        self.base_test_adjustment_price(data)

    def test_co_surgeons_mod_62_2(self):
        data = [
            (2507.128993472, {"code": 15757, "mod1": 62}, 0.625)
        ]
        self.base_test_adjustment_price(data)

    def test_co_surgeons_mod_62_9(self):
        data = [
            (43.305354623999996, {"code": 15850, "mod1": 62}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 66: team surgeons. Priced by report
    def test_team_surgeons_mod_66_0(self):
        data = [
            (694.9998027519999, {"code": 15920, "mod1": 66}, 0.00)
        ]
        self.base_test_adjustment_price(data)

    def test_team_surgeons_mod_66_1(self):
        data = [
            (1161.64482688, {"code": 22904, "mod1": 66}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    def test_team_surgeons_mod_66_2(self):
        data = [
            (5298.072014783999, {"code": 33945, "mod1": 66}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    def test_team_surgeons_mod_66_9(self):
        data = [
            (43.305354623999996, {"code": 15850, "mod1": 66}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier 78: return to the operating room for related procedure
    # wasn't described

    # - Modifier TC: Technical component only
    # see test_multiple_procedure_adjustments_7

    # - Modifier QX: CRNA service under supervision of physician. 50% of MPFS allowable
    def test_crna_mod_qx(self):
        data = [
            (1022.520501568, {"code": 15832, "mod1": 'QX'}, 0.50)
        ]
        self.base_test_adjustment_price(data)

    # - Modifier QY: CRNA service under supervision of anesthesiologist
    def test_crna_mod_qy(self):
        data = [
            (43.305354623999996, {"code": 15850, "mod1": 'QY'}, 0.50)
        ]
        self.base_test_adjustment_price(data)

    # - Multiple Procedure Payment Reduction: HCPCS designated as “therapy only” have a reduced practice expense
    # portion of the allowable see test_multiple_procedure_adjustments_5

    # - Nurse Practitioners and Clinical Nursing Specialist Services: Payable at 85% of MPFS.
    #     - Identification of this provider type can be done through Taxonomy Code
    def test_nurse_practitioners_services(self):
        data = [
            (
                2403.4995115519996, {"rendering_provider_npi": '1992996136'}, 0.85,
                {"carrier": 9102, "locality_code": 99}),
            (2262.595247168, {"rendering_provider_npi": '1992996912'}, 0.85, {"carrier": 13282}),
            (2712.0504339199997, {"rendering_provider_npi": '1992995302'}, 0.85, {"carrier": 9102, "locality_code": 4}),
            (
                2571.2003039359993, {"rendering_provider_npi": '1992867873'}, 0.85,
                {"carrier": 14212, "locality_code": 1}),
            (2342.4001796479997, {"rendering_provider_npi": '1992734073'}, 0.85, {"carrier": 2402}),
            (2494.963189312, {"rendering_provider_npi": '1992998066'}, 0.85, {"carrier": 8202, "locality_code": 1}),
            (2365.741128448, {"rendering_provider_npi": '1992979793'}, 0.85, {"carrier": 10212, "locality_code": 1}),
            (2248.3889370239995, {"rendering_provider_npi": '1992975395'}, 0.85, {"carrier": 6202}),
            (2376.2832615039997, {"rendering_provider_npi": '1992976898'}, 0.85, {"carrier": 4112}),
            (2425.839695744, {"rendering_provider_npi": '1992999593'}, 0.85, {"carrier": 12302}),
            (2248.3889370239995, {"rendering_provider_npi": '1992772107'}, 0.85, {"carrier": 6202}),
            (2262.595247168, {"rendering_provider_npi": '1992999395'}, 0.85, {"carrier": 13282}),
            (2308.108924416, {"rendering_provider_npi": '1982725214'}, 0.85, {"carrier": 12502, "locality_code": 99}),
            (
                2297.8107570559996, {"rendering_provider_npi": '1992956817'}, 0.85,
                {"carrier": 8202, "locality_code": 99}),
            (2672.263815296, {"rendering_provider_npi": '1992330369'}, 0.85, {"carrier": 12202}),
            (2330.71328448, {"rendering_provider_npi": '1992984868'}, 0.85, {"carrier": 5302, "locality_code": 2}),
            (2152.6605512319998, {"rendering_provider_npi": '1992942353'}, 0.85, {"carrier": 7302}),
            (2169.236143616, {"rendering_provider_npi": '1992950075'}, 0.85, {"carrier": 2202})
        ]
        self.base_test_adjustment_price(data)

    def test_clinical_nursing_specialist_services(self):
        data = [
            (
                2533.9233560959997, {"rendering_provider_npi": '1992976492'}, 0.85,
                {"carrier": 6102, "locality_code": 15}),
            (2217.4334435199994, {"rendering_provider_npi": '1992957575'}, 0.85, {"carrier": 10112}),
            (
                2329.2072654719996, {"rendering_provider_npi": '1992971154'}, 0.85,
                {"carrier": 4412, "locality_code": 31}),
            (2184.0945928319998, {"rendering_provider_npi": '1992936777'}, 0.85, {"carrier": 10312}),
            (2200.2760867839997, {"rendering_provider_npi": '1992060388'}, 0.85, {"carrier": 5202}),
            (2365.741128448, {"rendering_provider_npi": '1982153359'}, 0.85, {"carrier": 10212, "locality_code": 1}),
            (
                2261.8961916159997, {"rendering_provider_npi": '1982909024'}, 0.85,
                {"carrier": 4412, "locality_code": 99}),
            (2217.4334435199994, {"rendering_provider_npi": '1992992200'}, 0.85, {"carrier": 10112}),
            (
                2261.8961916159997, {"rendering_provider_npi": '1992266241'}, 0.85,
                {"carrier": 4412, "locality_code": 99}),
            (2307.8978002559998, {"rendering_provider_npi": '1992318182'}, 0.85, {"carrier": 15202}),
            (
                2571.2003039359993, {"rendering_provider_npi": '1942593942'}, 0.85,
                {"carrier": 14212, "locality_code": 1}),
            (
                2521.0635489279994, {"rendering_provider_npi": '1629421367'}, 0.85,
                {"carrier": 1182, "locality_code": 18}),
            (2521.231365568, {"rendering_provider_npi": '1992906077'}, 0.85, {"carrier": 12502, "locality_code": 1}),
            (
                2303.4945081599994, {"rendering_provider_npi": '1982908166'}, 0.85,
                {"carrier": 4412, "locality_code": 28}),
            (2262.595247168, {"rendering_provider_npi": '1992136485'}, 0.85, {"carrier": 13282}),
            (2423.88219584, {"rendering_provider_npi": '1962478479'}, 0.85, {"carrier": 1112, "locality_code": 62}),
            (
                2261.8961916159997, {"rendering_provider_npi": '1003134321'}, 0.85,
                {"carrier": 4412, "locality_code": 99}),
            (2333.9927464319994, {"rendering_provider_npi": '1992771513'}, 0.85, {"carrier": 4212}),
            (2442.19947232, {"rendering_provider_npi": '1992938781'}, 0.85, {"carrier": 14212, "locality_code": 99}),
            (2308.108924416, {"rendering_provider_npi": '1992977664'}, 0.85, {"carrier": 12502, "locality_code": 99}),
            (2155.1377413759997, {"rendering_provider_npi": '1982031514'}, 0.85, {"carrier": 7102}),
            (2169.236143616, {"rendering_provider_npi": '1992130421'}, 0.85, {"carrier": 2202}),
            (2177.277628288, {"rendering_provider_npi": '1952378895'}, 0.85, {"carrier": 8102}),
            (2335.925705408, {"rendering_provider_npi": '1992026538'}, 0.85, {"carrier": 5302, "locality_code": 1}),
            (2627.1337704959997, {"rendering_provider_npi": '1871643403'}, 0.85, {"carrier": 1112, "locality_code": 7}),
            (2399.6523601920003, {"rendering_provider_npi": '1992800015'}, 0.85, {"carrier": 14312}),
            (2307.8978002559998, {"rendering_provider_npi": '1922030972'}, 0.85, {"carrier": 15202}),
            (3067.609864768, {"rendering_provider_npi": '1972142131'}, 0.85, {"carrier": 2102}),
            (
                2297.8107570559996, {"rendering_provider_npi": '1790338481'}, 0.85,
                {"carrier": 8202, "locality_code": 99}),
            (2248.3889370239995, {"rendering_provider_npi": '1992990881'}, 0.85, {"carrier": 6202}),
            (2274.3243671679998, {"rendering_provider_npi": '1952723603'}, 0.85,
             {"carrier": 10212, "locality_code": 99}),
            (2519.351819199999, {"rendering_provider_npi": '1992737449'}, 0.85, {"carrier": 13102}),
            (2384.1814704639996, {"rendering_provider_npi": '1144400771'}, 0.85, {"carrier": 1182, "locality_code": 73})
        ]
        self.base_test_adjustment_price(data)

    def test_crna(self):
        data = [
            (2308.108924416, {"rendering_provider_npi": '1992994859'}, 0.85, {"carrier": 12502, "locality_code": 99})
        ]
        self.base_test_adjustment_price(data)

    # - Nutrition and Dietician Services: Payable at 85% of MPFS.
    #     - Identification of this provider type can be done through Taxonomy Code
    def test_dietician_services(self):
        data = [
            (2494.963189312, {"rendering_provider_npi": '1992980981'}, 0.85, {"carrier": 8202, "locality_code": 1}),
            (2438.820403072, {"rendering_provider_npi": '1992972921'}, 0.85, {"carrier": 14412}),
            (
                2533.9233560959997, {"rendering_provider_npi": '1992999742'}, 0.85,
                {"carrier": 6102, "locality_code": 15}),
            (2330.71328448, {"rendering_provider_npi": '1992967806'}, 0.85, {"carrier": 5302, "locality_code": 2}),
            (2261.8961916159997, {"rendering_provider_npi": '1992985857'}, 0.85, {"carrier": 4412}),
            (2341.19911776, {"rendering_provider_npi": '1992917041'}, 0.85, {"carrier": 11302}),
            (2376.2832615039997, {"rendering_provider_npi": '1992331409'}, 0.85, {"carrier": 4112}),
            (2200.2760867839997, {"rendering_provider_npi": '1982235784'}, 0.85, {"carrier": 5202}),
            (2261.3754186879996, {"rendering_provider_npi": '1992319891'}, 0.85, {"carrier": 11502}),
            (2593.632877503999, {"rendering_provider_npi": '1629682232'}, 0.85, {"carrier": 6102, "locality_code": 16}),
            (2376.2832615039997, {"rendering_provider_npi": '1992312334'}, 0.85, {"carrier": 4112})
        ]
        self.base_test_adjustment_price(data)

    # - Physician Assistant Services: Reimbursement can occur in all POS settings as long as no facility charges are
    # paid in connection with the service. Payment is lesser of 80% of submitted charge or 85% of MPFS
    def test_physician_assistant_services(self):
        data = [
            (2593.632877503999, {"rendering_provider_npi": '1992999148'}, 0.85,
             {"carrier": 6102, "locality_code": 16, "charges_multiplier": 0.80}),
            (2308.108924416, {"rendering_provider_npi": '1992999882'}, 0.85,
             {"carrier": 12502, "locality_code": 99, "charges_multiplier": 0.80}),
            (2765.1717988479995, {"rendering_provider_npi": '1992998330'}, 0.85,
             {"carrier": 13202, "charges_multiplier": 0.80})
        ]
        self.base_test_adjustment_price(data)

    # - NCCI Edits
    def test_ncci_is_effective_date_between_deletion_date(self):
        data = [
            (date(2021, 8, 10), date(2021, 8, 10), date(2021, 8, 10), True),
            (date(2021, 8, 10), date(2021, 8, 11), date(2021, 8, 12), True),
            (date(2021, 8, 10), date(2021, 8, 11), date(2021, 8, 12), True),
            (date(2021, 8, 11), date(2021, 8, 10), date(2021, 8, 12), False),
            (None, date(2021, 8, 11), date(2021, 8, 12), False),
            (date(2021, 8, 10), date(2021, 8, 11), None, True),
            (date(2021, 8, 10), date(2021, 8, 10), None, True),
            (date(2021, 8, 10), None, None, False),
        ]

        for effective_date, service_date, deletion_date, res in data:
            self.assertEqual(
                utils.is_date_between_dates(
                    service_date, effective_date, deletion_date
                ), res
            )

    def test_ncci_adjustments_different_service_date(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/02/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_if_service_date_is_not_between_affective_and_deletion_dates(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '09/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '09/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_switch_line_items(self):
        data = [
            [
                (27.73, {'code': '99212', 'service_date': '01/01/2020'}),
                (491.54, {'code': '46948', 'service_date': '01/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 27.73 + 491.54)

    def test_ncci_adjustments_one_line_item(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '01/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13)

    def test_ncci_adjustments_three_line_items(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '01/01/2020'}),
                (86.97, {'code': '64451', 'service_date': '01/01/2020'}),
                (2012.13, {'code': '57112', 'service_date': '01/02/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 * 2)

    def test_ncci_adjustments_four_line_items(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '01/01/2020'}),
                (86.97, {'code': '64451', 'service_date': '01/01/2020'}),
                (2012.13, {'code': '57112', 'service_date': '01/02/2020'}),
                (86.97, {'code': '64451', 'service_date': '01/02/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 * 2)

    def test_ncci_adjustments_five_line_items(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '01/01/2020'}),
                (86.97, {'code': '64451', 'service_date': '01/01/2020'}),
                (2012.13, {'code': '57112', 'service_date': '01/02/2020'}),
                (86.97, {'code': '64451', 'service_date': '01/02/2020'}),
                (2012.13, {'code': '57112', 'service_date': '01/03/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 * 3)

    def test_ncci_adjustments_modifier_equals_0(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '01/01/2020'}),
                (86.97, {'code': '64451', 'service_date': '01/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    def test_ncci_adjustments_modifier_equals_9(self):
        data = [
            [
                (491.54, {'code': '46948', 'service_date': '01/01/2020'}),
                (27.73, {'code': '99212', 'service_date': '01/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 491.54 + 0)

    def test_ncci_adjustments_modifier_equals_1_and_mods_not_equals_any_of_59_XE_XS_XU_or_XP(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    def test_ncci_adjustments_modifier_equals_1_and_second_item_mod_1_equals_59(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020', 'mod1': '59'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_modifier_equals_1_and_first_item_mod_1_equals_59(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020', 'mod1': '59'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    def test_ncci_adjustments_modifier_equals_1_and_second_item_mod_1_equals_XE(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020', 'mod1': 'XE'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_modifier_equals_1_and_first_item_mod_1_equals_XE(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020', 'mod1': 'XE'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    def test_ncci_adjustments_modifier_equals_1_and_second_item_mod_1_equals_XS(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020', 'mod1': 'XS'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_modifier_equals_1_and_first_item_mod_1_equals_XS(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020', 'mod1': 'XS'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    def test_ncci_adjustments_modifier_equals_1_and_second_item_mod_1_equals_XU(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020', 'mod1': 'XU'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_modifier_equals_1_and_first_item_mod_1_equals_XU(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020', 'mod1': 'XU'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    def test_ncci_adjustments_modifier_equals_1_and_second_item_mod_1_equals_XP(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020', 'mod1': 'XP'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 9.89)

    def test_ncci_adjustments_modifier_equals_1_and_first_item_mod_1_equals_XP(self):
        data = [
            [
                (2012.13, {'code': '57112', 'service_date': '10/01/2020', 'mod1': 'XP'}),
                (9.89, {'code': '99211', 'service_date': '10/01/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 2012.13 + 0)

    # Multiple procedure adjustments
    # 0 = No adjustment for multiple procedures (all procedures paid at 100%). Base payment off lower of billed charge
    # amount or fee schedule calculation
    def test_multiple_procedure_adjustments_0(self):
        data = [
            [
                (35.116985279999994, {'code': 20979, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (35.116985279999994, {'code': 20979, 'service_date': '09/01/2020'})
            ],
            [
                (636.556665408, {'code': 33257, 'mod1': 'QY', 'service_date': '09/05/2020'}),
                (636.556665408, {'code': 33257, 'service_date': '09/05/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 1007.52)

    # 2 = If another procedure is reported on the same day, with an indicator of 2 or 3, rank procedures by fee schedule
    # price and apply reduction based on rank (1st 100%, 2nd 50%, 3rd – n 25%). Payment for all procedures is the lower
    # of billed charge or adjusted fee schedule amount.
    def test_multiple_procedure_adjustments_2(self):
        data_2 = [
            [
                (8.295916351999999, {'code': 11719, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (17.306045887999996, {'code': 11055, 'mod1': 'QY', 'service_date': '09/01/2020'}),
                (33.298069446, {'code': 11900, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (58.56151123199999, {'code': 10040, 'mod1': 'QY', 'service_date': '09/01/2020'}),
                (8.295916351999999, {'code': 11719, 'service_date': '09/01/2020'}),
            ],
            [
                (33.298069446, {'code': 11900, 'mod1': 'QX', 'service_date': '09/05/2020'}),
                (17.306045887999996, {'code': 11055, 'mod1': 'QY', 'service_date': '09/05/2020'}),
                (58.56151123199999, {'code': 10040, 'service_date': '09/05/2020'}),
            ],
            [
                (17.306045887999996, {'code': 11055, 'mod1': 'QX', 'service_date': '09/07/2020'}),
                (58.56151123199999, {'code': 10040, 'service_date': '09/07/2020'}),
            ],
            [
                (58.56151123199999, {'code': 10040, 'mod1': 'QY', 'service_date': '09/15/2020'}),
            ]
        ]
        data_3 = [
            [
                # endo_base 29830
                (548.019854208, {'code': 29834, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (566.2988757119999, {'code': 29835, 'mod1': '81', 'service_date': '09/01/2020'}),
                # endo_base 29805
                (596.822015808, {'code': 29820, 'mod1': '80', 'service_date': '09/01/2020'}),
                (641.7734170879997, {'code': 29822, 'service_date': '09/01/2020'}),
                (1147.533071488, {'code': 29807, 'mod1': 'QY', 'service_date': '09/01/2020'}),
                (1018.3460175359999, {'code': 29828, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (1018.3460175359999, {'code': 29828, 'mod1': '81', 'service_date': '09/01/2020'}),
            ],
            [
                # endo_base 29830
                (548.019854208, {'code': 29834, 'mod1': 'QX', 'service_date': '09/05/2020'}),
                # endo_base 29805
                (1018.3460175359999, {'code': 29828, 'service_date': '09/05/2020'}),
                (1147.533071488, {'code': 29807, 'mod1': 'QY', 'service_date': '09/05/2020'}),
            ],
            [
                # endo_base 29805
                (596.822015808, {'code': 29820, 'mod1': 'AS', 'service_date': '09/07/2020'}),
                (1147.533071488, {'code': 29807, 'service_date': '09/07/2020'}),
            ],
            [
                # endo_base 29805
                (1018.3460175359999, {'code': 29828, 'mod1': 'QY', 'service_date': '09/15/2020'}),
            ]
        ]
        with self.subTest("multi_proc 2 only"):
            self.base_test_multiple_procedure_adjustment_price_pre_calc(data_2, lambda prices: prices[0] + 0.50 * sum(
                prices[1:2]) + 0.25 * sum(prices[2:]))
            self.base_test_multiple_procedure_adjustment_price(data_2, 204.08)
        with self.subTest("multi_proc 3 only"):
            self.base_test_multiple_procedure_adjustment_price(data_3, 4419.7)
        with self.subTest("multi_proc 2 and 3"):
            self.base_test_multiple_procedure_adjustment_price(data_2 + data_3, 4503.49)

    def test_multiple_procedure_adjustments_3(self):
        data = [
            [
                # endo_base 29830
                (548.019854208, {'code': 29834, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (566.2988757119999, {'code': 29835, 'mod1': '81', 'service_date': '09/01/2020'}),
            ],
            [
                # endo_base 29805
                (596.822015808, {'code': 29820, 'mod1': '80', 'service_date': '09/02/2020'}),
                (1147.533071488, {'code': 29807, 'mod1': 'QY', 'service_date': '09/02/2020'}),
                (1018.3460175359999, {'code': 29828, 'mod1': '81', 'service_date': '09/02/2020'}),
                (1018.3460175359999, {'code': 29828, 'mod1': 'QX', 'service_date': '09/02/2020'}),
                (641.7734170879997, {'code': 29822, 'service_date': '09/02/2020'}),
            ],
            [
                # endo_base 29830
                (548.019854208, {'code': 29834, 'mod1': 'QX', 'service_date': '09/05/2020'}),
            ],
            [
                # endo_base 29805
                (596.822015808, {'code': 29820, 'service_date': '09/06/2020'}),
                (1147.533071488, {'code': 29807, 'mod1': 'QY', 'service_date': '09/06/2020'}),
            ],
            [
                # endo_base 29805
                (596.822015808, {'code': 29820, 'mod1': 'AS', 'service_date': '09/07/2020'}),
                (1147.533071488, {'code': 29807, 'service_date': '09/07/2020'}),
            ],
            [
                # endo_base 29805
                (1018.3460175359999, {'code': 29828, 'mod1': 'QY', 'service_date': '09/15/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: prices[0] + 0.50 * sum(
            prices[1:2]) + 0.25 * sum(prices[2:]))
        self.base_test_multiple_procedure_adjustment_price(data, 4294.84)

    # 4 = When multiple diagnostic imaging codes with TC modifiers are billed from the same family (i.e. the same base
    # procedure code in the diagnostic imaging column), rank procedures based on their fee schedule amount and apply
    # reductions based on rank (1st 100%, 2nd – n 50%). Final payment is lower of billed charge or adjusted fee schedule
    # amount. There are other reductions here but for the purposes of a first pass, we can build out this part.
    def test_multiple_procedure_adjustments_4(self):
        data = [
            [
                (279.544989056, {'code': 70482, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (279.544989056, {'code': 70482, 'mod1': 'AS', 'service_date': '09/01/2020'}),
                (449.88934463999993, {'code': 71550, 'mod1': '80', 'service_date': '09/01/2020'}),
                (348.49922239999995, {'code': 70336, 'mod1': '81', 'service_date': '09/01/2020'}),
                (88.66709465599999, {'code': 76604, 'service_date': '09/01/2020'}),
            ],
            [
                (279.544989056, {'code': 70482, 'mod1': 'QX', 'service_date': '09/05/2020'}),
                (449.88934463999993, {'code': 71550, 'mod1': '81', 'service_date': '09/05/2020'}),
                (348.49922239999995, {'code': 70336, 'service_date': '09/05/2020'}),
            ],
            [
                (348.49922239999995, {'code': 70336, 'mod1': 'QY', 'service_date': '09/07/2020'}),
                (88.66709465599999, {'code': 76604, 'service_date': '09/07/2020'}),
            ],
            [
                (88.66709465599999, {'code': 76604, 'mod1': 'AS', 'service_date': '09/15/2020'}),
            ]
        ]
        data_tc = [
            [
                (211.48866495999997, {'code': 70482, 'mod1': 'QX', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (211.48866495999997, {'code': 70482, 'mod1': 'AS', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (370.81703104, {'code': 71550, 'mod1': '80', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (268.68346303999994, {'code': 70336, 'mod1': '81', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (57.537288383999986, {'code': 76604, 'mod1': 'TC', 'service_date': '09/01/2020'}),
            ],
            [
                (211.48866495999997, {'code': 70482, 'mod1': 'QX', 'mod2': 'TC', 'service_date': '09/05/2020'}),
                (370.81703104, {'code': 71550, 'mod1': '81', 'mod2': 'TC', 'service_date': '09/05/2020'}),
                (268.68346303999994, {'code': 70336, 'mod1': 'TC', 'service_date': '09/05/2020'}),
            ],
            [
                (268.68346303999994, {'code': 70336, 'mod1': 'QY', 'mod2': 'TC', 'service_date': '09/07/2020'}),
                (57.537288383999986, {'code': 76604, 'mod1': 'TC', 'service_date': '09/07/2020'}),
            ],
            [
                (57.537288383999986, {'code': 76604, 'mod1': 'AS', 'mod2': 'TC', 'service_date': '09/15/2020'}),
            ]
        ]
        with self.subTest("No TC Only"):
            self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
            self.base_test_multiple_procedure_adjustment_price(data, 1229.4293212216319)
        with self.subTest("TC Only"):
            self.base_test_multiple_procedure_adjustment_price_pre_calc(data_tc, lambda prices: prices[0] + 0.50 * sum(
                prices[1:]))
            self.base_test_multiple_procedure_adjustment_price(data_tc, 722.2106841111039)
        with self.subTest("TC and No TC"):
            self.base_test_multiple_procedure_adjustment_price(data + data_tc, 1229.4293212216319 + 722.2106841111039)

    # 5 = 50% reduction to the practice expense component of certain therapy services
    def test_multiple_procedure_adjustments_5(self):
        data = [
            [
                (20.14341024, {'code': 97022, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (20.14341024, {'code': 97022, 'mod1': 'AS', 'service_date': '09/01/2020'}),
                (13.570050495999997, {'code': 97016, 'mod1': '80', 'service_date': '09/01/2020'}),
                (13.570050495999997, {'code': 97016, 'mod1': '81', 'service_date': '09/01/2020'}),
                (20.14341024, {'code': 97022, 'service_date': '09/01/2020'}),
            ],
            [
                (20.14341024, {'code': 97022, 'mod1': 'QX', 'service_date': '09/05/2020'}),
                (13.570050495999997, {'code': 97016, 'mod1': 'AS', 'service_date': '09/05/2020'}),
                (13.570050495999997, {'code': 97016, 'service_date': '09/05/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: 0.5 * sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 31.41)

    # 6 = 25% reduction of the second highest and subsequent procedures of diagnostic cardiovascular
    # services (1st 100%, 2nd – n 75%)
    def test_multiple_procedure_adjustments_6(self):
        data = [
            [
                (541.77346624, {'code': 78452, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (541.77346624, {'code': 78452, 'mod1': 'AS', 'service_date': '09/01/2020'}),
                (230.74535372799994, {'code': 78466, 'mod1': '80', 'service_date': '09/01/2020'}),
                (145.628032128, {'code': 75827, 'mod1': '81', 'service_date': '09/01/2020'}),
                (145.628032128, {'code': 75827, 'service_date': '09/01/2020'}),
            ],
            [
                (230.74535372799994, {'code': 78466, 'mod1': 'QX', 'service_date': '09/05/2020'}),
                (541.77346624, {'code': 78452, 'mod1': '81', 'service_date': '09/05/2020'}),
                (541.77346624, {'code': 78452, 'service_date': '09/05/2020'}),
            ],
            [
                (541.77346624, {'code': 78452, 'mod1': 'QY', 'service_date': '09/07/2020'}),
                (230.74535372799994, {'code': 78466, 'service_date': '09/07/2020'}),
            ],
            [
                (230.74535372799994, {'code': 78466, 'mod1': 'AS', 'service_date': '09/15/2020'}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: prices[0] + 0.75 * sum(
            prices[1:]))
        self.base_test_multiple_procedure_adjustment_price(data, 1649.1763634350077)

    # 7 = Subject to 20% reduction of the second highest and subsequent procedures to the TC of diagnostic ophthalmology
    # services (1st 100%, 2nd – n 80%)
    def test_multiple_procedure_adjustments_7(self):
        data = [
            [
                (13.242717823999998, {'code': 76514, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (13.242717823999998, {'code': 76514, 'mod1': 'AS', 'service_date': '09/01/2020'}),
                (13.242717823999998, {'code': 76514, 'mod1': '80', 'service_date': '09/01/2020'}),
                (13.242717823999998, {'code': 76514, 'mod1': '81', 'service_date': '09/01/2020'}),
                (13.242717823999998, {'code': 76514, 'service_date': '09/01/2020'}),
            ],
            [
                (41.067077632, {'code': 92025, 'mod1': 'QX', 'service_date': '09/05/2020'}),
                (41.067077632, {'code': 92025, 'mod1': '81', 'service_date': '09/05/2020'}),
                (41.067077632, {'code': 92025, 'service_date': '09/05/2020'}),
            ],
            [
                (70.45556070399999, {'code': 92060, 'mod1': 'QY', 'service_date': '09/07/2020'}),
                (70.45556070399999, {'code': 92060, 'service_date': '09/07/2020'}),
            ],
            [
                (25.016228032, {'code': 76516, 'mod1': '26', 'service_date': '09/15/2020'}),
            ]
        ]
        data_tc = [
            [
                (4.427833023999999, {'code': 76514, 'mod1': 'QX', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (4.427833023999999, {'code': 76514, 'mod1': 'AS', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (4.427833023999999, {'code': 76514, 'mod1': '80', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (4.427833023999999, {'code': 76514, 'mod1': '81', 'mod2': 'TC', 'service_date': '09/01/2020'}),
                (4.427833023999999, {'code': 76514, 'mod1': 'TC', 'service_date': '09/01/2020'}),
            ],
            [
                (19.543601087999996, {'code': 92025, 'mod1': 'QX', 'mod2': 'TC', 'service_date': '09/05/2020'}),
                (19.543601087999996, {'code': 92025, 'mod1': '81', 'mod2': 'TC', 'service_date': '09/05/2020'}),
                (19.543601087999996, {'code': 92025, 'mod1': 'TC', 'service_date': '09/05/2020'}),
            ],
            [
                (29.348423615999994, {'code': 92060, 'mod1': 'QY', 'mod2': 'TC', 'service_date': '09/07/2020'}),
                (29.348423615999994, {'code': 92060, 'mod1': 'TC', 'service_date': '09/07/2020'}),
            ],
            [
                (28.939889343999994, {'code': 76516, 'mod1': 'TC', 'service_date': '09/15/2020'}),
            ]
        ]
        with self.subTest("No TC Only"):
            self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
            self.base_test_multiple_procedure_adjustment_price(data, 224.78)
        with self.subTest("TC Only"):
            self.base_test_multiple_procedure_adjustment_price_pre_calc(data_tc, lambda prices: prices[0] + 0.80 * sum(
                prices[1:]))
            self.base_test_multiple_procedure_adjustment_price(data_tc, 107.71)
        with self.subTest("TC and No TC"):
            self.base_test_multiple_procedure_adjustment_price(data + data_tc, 224.78 + 107.71)

    # 9 = concept does not apply
    def test_multiple_procedure_adjustments_9(self):
        data = [
            [
                (361.01762195199996, {'code': 22526, 'mod1': 'QX', 'service_date': '09/01/2020'}),
                (361.01762195199996, {'code': 22526, 'service_date': '09/01/2020'})
            ],
            [
                (1079.3612606719998, {'code': 37216, 'mod1': 'QY', 'service_date': '09/01/2020'}),
                (1079.3612606719998, {'code': 37216, 'service_date': '09/01/2020'})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 2160.5683239359996)

    # Bilateral surgery
    def test_bilateral_surgery_mod_50_0(self):
        data = [
            (58.56151123199999, {"code": 10040, "mod1": 50}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_2units_0(self):
        data = [
            (58.56151123199999, {"code": 10040, "quantity": 2}, 1.00 * 2)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_lt_rt_0_with_adjustments(self):
        data = [
            [
                (58.56151123199999, {'code': 10040, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (58.56151123199999, {'code': 10040, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ]
        ]
        # pre calculation is not valid when multi_proc=2
        # self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 87.84226684799998)

    def test_bilateral_surgery_lt_rt_0_no_adjustments(self):
        data = [
            [
                (1224.2184189439997, {'code': 63620, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (1224.2184189439997, {'code': 63620, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 2448.4368378879994)

    def test_bilateral_surgery_mod_50_1(self):
        data = [
            (643.2530906879999, {"code": 27052, "mod1": 50}, 1.50)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_2units_1(self):
        data = [
            (160.003963392, {"code": 67335, "quantity": 2}, (1.50 / 2) * 2)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_lt_rt_1_with_adjustments(self):
        data = [
            [
                (643.2530906879999, {'code': 27052, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (643.2530906879999, {'code': 27052, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ],
            [
                (160.003963392, {'code': 67335, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ],
            [
                (643.2530906879999, {'code': 27052, 'mod1': 'RT', 'service_date': '09/05/2020'}),
            ]
        ]
        # pre calculation is not valid when multi_proc=2
        # self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: 1.5 * min(prices) if len(
        #     prices) > 1 else sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 1526.91)

    def test_bilateral_surgery_lt_rt_1_no_adjustments(self):
        data = [
            [
                (160.003963392, {'code': 67335, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (160.003963392, {'code': 67335, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ],
            [
                (643.2530906879999, {'code': 27052, 'mod1': 'RT', 'service_date': '09/01/2020'}),
            ],
            [
                (160.003963392, {'code': 67335, 'mod1': 'RT', 'service_date': '09/05/2020'})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: 1.5 * min(prices) if len(
            prices) > 1 else sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 1043.25)

    def test_bilateral_surgery_mod_50_2(self):
        data = [
            (358.540070912, {"code": 63295, "mod1": 50}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_2units_2(self):
        data = [
            (358.540070912, {"code": 63295, "quantity": 2}, 1.00 * 2)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_lt_rt_2(self):
        data = [
            [
                (358.540070912, {'code': 63295, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (358.540070912, {'code': 63295, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 717.080141824)

    def test_bilateral_surgery_mod_50_3(self):
        data = [
            (145.09679321599998, {"code": 73615, "mod1": 50}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_2units_3(self):
        data = [
            (145.09679321599998, {"code": 73615, "quantity": 2}, 1.00 * 2)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_lt_rt_3(self):
        data = [
            [
                (145.09679321599998, {'code': 73615, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (145.09679321599998, {'code': 73615, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 290.20)

    def test_bilateral_surgery_mod_50_9(self):
        data = [
            (43.305354623999996, {"code": 15850, "mod1": 50}, 1.00)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_2units_9(self):
        data = [
            (43.305354623999996, {"code": 15850, "quantity": 2}, 1.00 * 2)
        ]
        self.base_test_adjustment_price(data)

    def test_bilateral_surgery_lt_rt_9(self):
        data = [
            [
                (43.305354623999996, {'code': 15850, 'mod1': 'LT', 'service_date': '09/01/2020'}),
                (43.305354623999996, {'code': 15850, 'mod1': 'RT', 'service_date': '09/01/2020'})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price_pre_calc(data, lambda prices: sum(prices))
        self.base_test_multiple_procedure_adjustment_price(data, 86.62)

    # Additional tests

    def test_mod_as_rvu_0_mod_80_rvu_0(self):
        data = [
            (1285.882192192, {"code": 21249, "mod1": 'AS', "mod2": 80}, 0.16 * 0.85)
        ]
        self.base_test_adjustment_price(data)

    # Anesthesia pricing tests

    def test_anesthesia_base(self):
        data = [
            [
                (117.09, {"code": "00100"}, {"locality_code": 99})
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 117.09)

    def test_anesthesia_multiple_procedures(self):
        data = [
            [
                (117.09, {"code": "00100"}, {"locality_code": 99}),
                (93.98, {"code": "00126"}, {"locality_code": 99}),
            ],
            [
                (117.09, {"code": "00100"}, {"locality_code": 99}),
                (93.98, {"code": "00126"}, {"locality_code": 99}),
                (140.2, {"code": "00144"}, {"locality_code": 99}),
            ],
            [
                (117.09, {"code": "00100"}, {"locality_code": 99}),
                (93.98, {"code": "00126"}, {"locality_code": 99}),
                (140.2, {"code": "00144"}, {"locality_code": 99}),
                (1.54, {"code": "01999"}, {"locality_code": 99}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 1372.77)

    # Modifiers QX, QS, QK and QY
    # Action: Reduce payment by 50%
    def test_anesthesia_QX_QS_QK_QY(self):
        data = [
            [
                (117.09, {"code": "00100", "mod1": "QX"}, {"locality_code": 99}),
                (93.98, {"code": "00126", "mod3": "QS"}, {"locality_code": 99}),
                (140.2, {"code": "00144", "mod2": "QK"}, {"locality_code": 99}),
                (1.54, {"code": "01999", "mod2": "QY"}, {"locality_code": 99}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 579.28 / 2)

    # Modifier AA
    # Action: None; do not reduce payment
    def test_anesthesia_AA(self):
        data = [
            [
                (117.09, {"code": "00100", "mod1": "QX", "mod2": "AA"}, {"locality_code": 99}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 117.09)

    # Modifier AD (only occurs when more than 4 procedures are performed)
    # Action: Set Base Units = 3 and calculate payment normally per Multiple Anesthesia Procedures section
    def test_anesthesia_AD(self):
        data = [
            [
                (117.09, {"code": "00100", "mod1": "AD"}, {"locality_code": 99}),
                (117.09, {"code": "00100", "mod1": "AD"}, {"locality_code": 99}),
                (117.09, {"code": "00100", "mod1": "AD"}, {"locality_code": 99}),
                (117.09, {"code": "00100", "mod1": "AD"}, {"locality_code": 99}),
                (117.09, {"code": "00100", "mod1": "AD"}, {"locality_code": 99}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 385.15)

    # Modifier QZ
    # Action: None; do not reduce payment even if Taxonomy code for provider is for CRNA. If Taxonomy code is for CRNA,
    # do not reduce payment by 50%
    def test_anesthesia_QZ(self):
        data = [
            [
                (117.09, {"code": "00100", "mod1": "QX", "mod2": "QZ"}, {"locality_code": 99}),
            ]
        ]
        self.base_test_multiple_procedure_adjustment_price(data, 117.09)


if __name__ == '__main__':
    unittest.main()
