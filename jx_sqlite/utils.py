from jx_base import Snowflake
from mo_json.typed_encoder import untype_path


class BasicSnowflake(Snowflake):
    def __init__(self, query_paths, columns):
        self._query_paths = query_paths
        self._columns = columns

    @property
    def query_paths(self):
        return self._query_paths

    @property
    def columns(self):
        return self._columns

    @property
    def column(self):
        return ColumnLocator(self._columns)


class ColumnLocator(object):
    def __init__(self, columns):
        self.columns = columns

    def __getitem__(self, column_name):
        return [
            c
            for c in self.columns
            if untype_path(c.name) == column_name
        ]
