from datetime import datetime, timezone
from unittest.mock import patch

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.ipc as ipc  # type: ignore[import-untyped]

from cvec import CVec
from cvec.models.metric import MetricDataPoint


def test_arrow_upload_batches_share_schema() -> None:
    timestamp = datetime(2026, 1, 1, 12, 0, 0, 123, tzinfo=timezone.utc)
    numeric = MetricDataPoint(name="power", time=timestamp, value_double=0.0)
    text = MetricDataPoint(name="state", time=timestamp, value_string="idle")
    batches: list[list[MetricDataPoint]] = [[numeric], [text], [numeric, text], []]

    with (
        patch.object(CVec, "_fetch_config", return_value="unused"),
        patch.object(
            CVec, "_construct_email_from_api_key", return_value="sdk@example.test"
        ),
        patch.object(CVec, "_login_with_supabase"),
        patch.object(CVec, "_make_request") as request,
    ):
        client = CVec(host="https://metrics.example.test", api_key="unused")
        for batch in batches:
            client.add_metric_data(batch, use_arrow=True)

    assert request.call_count == len(batches)
    tables = []
    for call in request.call_args_list:
        assert call.args == ("POST", "/api/metrics/data/arrow")
        table = ipc.open_file(call.kwargs["data"]).read_all()
        tables.append(table)

    combined = pa.concat_tables(tables)
    assert combined.schema == pa.schema(
        [
            ("name", pa.string()),
            ("time", pa.timestamp("us", tz="UTC")),
            ("value_double", pa.float64()),
            ("value_string", pa.string()),
        ]
    )
    assert combined.to_pylist() == [
        point.model_dump() for point in (numeric, text, numeric, text)
    ]
