from uuid import UUID

try:
    from uuid import uuid7 as generate_uuid7

except ImportError:
    from uuid6 import uuid7 as generate_uuid7


def new_uuid7() -> UUID:
    value = generate_uuid7()

    if value.version != 7:
        raise RuntimeError(
            "UUID generator returned "
            f"version {value.version}, expected 7"
        )

    return value
