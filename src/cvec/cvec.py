import os
import pandas as pd


class CVec:
    """
    CVec API Client
    """

    def __init__(
        self,
        host=None,
        tenant=None,
        api_key=None,
        default_start_at=None,
        default_end_at=None,
    ):
        """
        Setup the SDK with the given host and API Key.
        The host and API key are loaded from environment variables CVEC_HOST,
        CVEC_TENANT, CVEC_API_KEY, if they are not given as arguments to the constructor.
        The default_start_at and default_end_at constrain most API keys, and can be overridden
        by the start_at and end_at arguments to each API function.
        """
        self.host = host or os.environ.get("CVEC_HOST")
        self.tenant = tenant or os.environ.get("CVEC_TENANT")
        self.api_key = api_key or os.environ.get("CVEC_API_KEY")
        self.default_start_at = default_start_at
        self.default_end_at = default_end_at

        if not self.host:
            raise ValueError(
                "CVEC_HOST must be set either as an argument or environment variable"
            )
        if not self.tenant:
            raise ValueError(
                "CVEC_TENANT must be set either as an argument or environment variable"
            )
        if not self.api_key:
            raise ValueError(
                "CVEC_API_KEY must be set either as an argument or environment variable"
            )

    def get_spans(self, tag_name, start_at=None, end_at=None, limit=None):
        """
        Return all of the time spans where a tag has a constant value
        within the specified [start_at, end_at) interval.
        The function returns a list of time-ranges with the value for each time-range.
        Returns a list of spans. Each span has the following fields:
        {id, tag_name, value, begin_at, end_at, raw_begin_at, raw_end_at, metadata}.
        In a future version of the SDK, spans can be annotated, edited, and deleted.
        """
        # Implementation to be added
        return []

    def get_metric_data(self, tag_names=None, start_at=None, end_at=None):
        """
        Return all data-points within a given [start_at, end_at) interval,
        optionally selecting a given list of tags.
        The return value is a Pandas DataFrame with three columns: tag_name, time, value.
        One row is returned for each tag value transition.
        """
        # Implementation to be added
        return pd.DataFrame(columns=["tag_name", "time", "value"])

    def get_tags(self, start_at=None, end_at=None):
        """
        Return a list of tags that had at least one transition in the given [start_at, end_at) interval.
        All tags are returned if no start_at and end_at are given.
        Each tag has {id, name, birth_at, death_at}.
        """
        # Implementation to be added
        return []
