"""Product-group attributes for newly created utilisation reports only."""

import re


def build_utilisation_attributes(group, pasport, config, production_date=None,
                                 expiration_date=None, *, utilisation_type=None):
    # API SUZ 3.0.40, section 4.4.11.1.19: SPLITMARK forbids all three fields.
    if utilisation_type == 'SPLITMARK':
        return {}

    attributes = {}
    if production_date:
        attributes['productionDate'] = production_date
    if expiration_date:
        attributes['expirationDate'] = expiration_date
    if group != 'chemistry':
        return attributes

    if 'alcoholVolume ' in pasport:
        raise ValueError('PasportData: use alcoholVolume without a trailing space')
    value = pasport.get('alcoholVolume')
    if value is None:
        settings = config.get('product_group_settings', {})
        if not isinstance(settings, dict) or not isinstance(settings.get(group, {}), dict):
            raise ValueError('product_group_settings.chemistry must be an object')
        chemistry = settings.get(group, {})
        if 'alcoholVolume ' in chemistry:
            raise ValueError('product_group_settings.chemistry: use alcoholVolume without a trailing space')
        value = chemistry.get('alcoholVolume')
        if value is None:
            raise ValueError(
                'Missing PasportData.alcoholVolume and '
                'product_group_settings.chemistry.alcoholVolume default'
            )

    # Never substitute zero for an explicit but invalid value, or round it.
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError('alcoholVolume must be a number from 0 to 99.9 with at most one decimal place')
    value = str(value)
    if re.fullmatch(r'(?:[0-9]{1,2}|[0-9]{1,2}\.[0-9])', value) is None:
        raise ValueError('alcoholVolume must be a number from 0 to 99.9 with at most one decimal place')
    attributes['alcoholVolume'] = value
    return attributes
