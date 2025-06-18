import io
from typing import List

import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd

from cvec.models.metric import MetricDataPoint


def metric_data_points_to_arrow(data_points: List[MetricDataPoint]) -> bytes:
    """
    Convert metric data points to Arrow format.
    
    Args:
        data_points: List of MetricDataPoint objects to convert
        
    Returns:
        bytes: Arrow IPC format data
    """
    # Create arrays for each field
    names = [point.name for point in data_points]
    times = [point.time for point in data_points]
    value_doubles = [point.value_double for point in data_points]
    value_strings = [point.value_string for point in data_points]

    # Create Arrow arrays
    names_array = pa.array(names)
    times_array = pa.array(times, type=pa.timestamp('us', tz='UTC'))
    value_doubles_array = pa.array(value_doubles)
    value_strings_array = pa.array(value_strings)

    # Create Arrow table
    table = pa.table({
        'name': names_array,
        'time': times_array,
        'value_double': value_doubles_array,
        'value_string': value_strings_array
    })

    # Convert to Arrow IPC format
    sink = pa.BufferOutputStream()
    with ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def arrow_to_metric_data_points(arrow_data: bytes) -> List[MetricDataPoint]:
    """
    Convert Arrow format to metric data points.
    
    Args:
        arrow_data: Arrow IPC format data
        
    Returns:
        List[MetricDataPoint]: List of converted metric data points
    """
    # Read Arrow data
    reader = ipc.open_file(io.BytesIO(arrow_data))
    table = reader.read_all()

    # Convert to list of MetricDataPoint
    data_points = []
    for i in range(len(table)):
        data_points.append(MetricDataPoint(
            name=table['name'][i].as_py(),
            time=table['time'][i].as_py(),
            value_double=table['value_double'][i].as_py(),
            value_string=table['value_string'][i].as_py()
        ))
    return data_points


def arrow_to_dataframe(arrow_data: bytes) -> pd.DataFrame:
    """
    Convert Arrow format directly to a pandas DataFrame.
    
    Args:
        arrow_data: Arrow IPC format data
        
    Returns:
        pd.DataFrame: DataFrame with columns [name, time, value_double, value_string]
    """
    # Read Arrow data
    reader = ipc.open_file(io.BytesIO(arrow_data))
    table = reader.read_all()
    
    # Convert directly to DataFrame
    return table.to_pandas() 