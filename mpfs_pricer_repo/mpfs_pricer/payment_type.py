def get_code_to_payment_mapping():
    facilty_code_to_payment_type = {
        '01': 'NF',
        '02': 'F',
        '03': 'NF',
        '04': 'NF',
        '09': 'NF',
        '11': 'NF',
        '12': 'NF',
        '13': 'NF',
        '14': 'NF',
        '15': 'NF',
        '16': 'NF',
        '17': 'NF',
        '19': 'F',
        '20': 'NF',
        '21': 'F',
        '22': 'F',
        '23': 'F',
        '24': 'F',
        '25': 'NF',
        '26': 'F',
        '31': 'F',
        '32': 'NF',
        '33': 'NF',
        '34': 'F',
        '41': 'F',
        '42': 'F',
        '49': 'NF',
        '50': 'NF',
        '51': 'F',
        '52': 'F',
        '53': 'F',
        '54': 'NF',
        '55': 'NF',
        '56': 'F',
        '57': 'NF',
        '60': 'NF',
        '61': 'F',
        '62': 'NF',
        '65': 'NF',
        '71': 'NF',
        '72': 'NF',
        '81': 'NF',
        '99': 'NF',
    }
    return facilty_code_to_payment_type


def requires_facilty_payment(place_of_service_code):
    """
    Returns true of a place of service code qualifies for facility payment, else false
    :param place_of_service_code:
    :return: Boolean
    """
    mapping = get_code_to_payment_mapping()
    if place_of_service_code in mapping:
        return mapping[place_of_service_code]
    else:
        raise ValueError(f"Invalid place of service code for MPFS payment: {place_of_service_code}")
