import pandas as pd

class CVec:
    """
    CVec API Client
    """

    def __init__(self, host=None, tenant=None, api_key=None, default_time_range=None):
        """
        Setup the SDK with the given host and API Key.
        The host and API key are loaded from environment variables CVEC_HOST,
        CVEC_TENANT, CVEC_API_KEY, if they are not given as arguments to the constructor.
        The default_time_range constrains most API keys, and can be overridden
        by the time_range argument to each API function.
        """
        # Implementation to be added
        pass

    def get_spans(self, tag_name, time_range=None, limit=None):
        """
        Return all of the time spans where a tag has a constant value.
        The function returns a list of time-ranges with the value for each time-range.
        Returns a list of spans. Each span has the following fields:
        {id, tag_name, value, begin_at, end_at, raw_begin_at, raw_end_at, metadata}.
        In a future version of the SDK, spans can be annotated, edited, and deleted.
        """
        # Implementation to be added
        return []

    def get_metric_data(self, tag_names=None, time_range=None):
        """
        Return all data-points within a given time-range, optionally selecting a given list of tags.
        The return value is a Pandas DataFrame with three columns: tag_name, time, value.
        One row is returned for each tag value transition.
        """
        # Implementation to be added
        return pd.DataFrame(columns=["tag_name", "time", "value"])

    def get_tags(self, time_range=None):
        """
        Return a list of tags that had at least one transition in the given time range.
        All tags are returned if no time_range is given.
        Each tag has {id, name, birth_at, death_at}.
        """
        # Implementation to be added
        return []
