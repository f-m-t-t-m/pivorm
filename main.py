import sqlite3
import inspect

class SqliteDatabase:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

    def _execute(self, sql):
        return self.cursor.execute(sql)

    def create(self, table):
        return self._execute(table._get_create_sql())


class Table:
    @classmethod
    def _get_name(cls):
        return cls.__name__.lower()

    @classmethod
    def _get_fields(cls):
        fields = []
        for name, field in inspect.getmembers(cls):
            if isinstance(field, BaseField):
                field_row = [name, field.type]
                settings = field.get_settings()
                if settings['unique']:
                    field_row.append("UNIQUE")
                if not settings['null']:
                    field_row.append("NOT NULL")
                if settings['default'] is not None:
                    field_row.append("DEFAULT {value}".format(value=settings['default']))
                fields.append(field_row)
        return fields

    @classmethod
    def _get_create_sql(cls):
        create_table_sql = "CREATE TABLE IF NOT EXISTS {table} ({fields})"
        fields = [["id", "INTEGER PRIMARY KEY"], *cls._get_fields()]
        fields = [" ".join(field) for field in fields]
        return create_table_sql.format(table=cls._get_name(),
                                       fields=", ".join(fields))


class BaseField:
    def __init__(self, unique=False, null=False, default=None):
        self.unique = unique
        self.null = null
        self.default = default

    def get_settings(self):
        return self.__dict__


class IntegerField(BaseField):
    type = "INTEGER"


class TextField(BaseField):
    type = "TEXT"


class RealField(BaseField):
    type = "REAL"


class BlobField(BaseField):
    type = "BLOB"


class Author(Table):
    name = TextField(unique=True, default=1)
    date = TextField()

db = SqliteDatabase("mydatabase.db")
db.create(Author)


