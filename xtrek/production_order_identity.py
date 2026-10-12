"""Flat production order identifiers shared by S3 keys and local filenames."""

import unicodedata


def safe_production_order_component(value):
    """Keep letters, numbers, combining marks and ``._-``; replace the rest.

    S3 accepts separators and many punctuation characters, but downstream
    filesystem and URL consumers cannot safely use them in an identifier.
    This conversion applies only to derived names, never to document fields.
    """
    result = ''.join(
        character if (
            character.isalnum()
            or unicodedata.category(character).startswith('M')
            or character in '._-'
        ) else '_'
        for character in str(value)
    ).rstrip('.')
    return result or 'unknown'
