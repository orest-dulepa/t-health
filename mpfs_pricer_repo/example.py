import json
from mpfs_pricer import pricer
import logging

logging.basicConfig(
    level=logging.DEBUG
)
claims = [
    {
        'claim_number': 'EXAMPLE-1234',
        'charges': 150.00,
        'npi': '1700883113',
        'service_from': '1/1/2020',
        'service_to': '1/1/2020',
        'line_items': [
            {
                'service_date': '1/1/2020',
                'place_of_service': '41',
                'code': '97162',
                'mod1': 'GP',
                'mod2': '',
                'mod3': '',
                'mod4': '',
                'charges': 150.00,
                'quantity': 1,
                'rendering_provider_npi': '1104863638',
            },
        ]
    }
]

for claim in claims:
    result = pricer.price_claim(claim)
    print(json.dumps(result, indent=2))
