from __future__ import annotations

import pytest

from hcsc.datalake.dre.store.names import validate_dq_database


@pytest.mark.parametrize("name", ["dq", "dq_test", "DQ2", "_dq"])
def test_plain_identifier_accepted(name: str) -> None:
    assert validate_dq_database(name) == name


@pytest.mark.parametrize(
    "name",
    ["", "dq.x", "`dq`", "dq; DROP TABLE x", "1dq", "dq db", "{{ dq_database }}", "dq-test", None],
)
def test_non_identifier_rejected(name: object) -> None:
    with pytest.raises(ValueError, match="plain identifier"):
        validate_dq_database(name)  # type: ignore[arg-type]
