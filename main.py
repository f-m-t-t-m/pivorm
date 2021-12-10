from __future__ import annotations
import sqlite3
import inspect
from abc import ABC, abstractmethod

SQL_TEMPLATE = {
    "CREATE": "CREATE TABLE IF NOT EXISTS {table} ({fields})",
    "INSERT": "INSERT INTO {name} ({fields}) VALUES ({values})",
    "SELECT_ALL": "SELECT * FROM {name}",
}


class MetaSingleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SqliteDatabase(metaclass=MetaSingleton):
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self, path):
        if self.connection is None:
            self.connection = sqlite3.connect(path)
            self.cursor = self.connection.cursor()

    def _execute(self, sql):
        return self.cursor.execute(sql)

    def create(self, table):
        return self._execute(table._get_create_sql())

    def save(self, instance):
        sql = instance._get_insert_sql()
        cursor = self._execute(sql)
        instance._data['id'] = cursor.lastrowid
        self.connection.commit()


class Table:
    def __init__(self, **kwargs):
        self._data = {
            "id": None
        }
        for k, v in kwargs.items():
            self._data[k] = v

    def __init_subclass__(cls, **kwargs):
        cls._bind_fields()
        cls.db = SqliteDatabase()
        cls.objects = Select(cls.db, cls)

    def __getattribute__(self, item):
        _data = object.__getattribute__(self, '_data')
        if item in _data:
            return _data[item]
        return object.__getattribute__(self, item)

    def get_data(self):
        return self._data

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
    def _bind_fields(cls):
        for name, field in inspect.getmembers(cls):
            if isinstance(field, BaseField):
                field.bind(cls, name)

    @classmethod
    def _get_create_sql(cls):
        fields = [["id", "INTEGER PRIMARY KEY"], *cls._get_fields()]
        fields = [" ".join(field) for field in fields]
        return SQL_TEMPLATE["CREATE"].format(table=cls._get_name(),
                                             fields=", ".join(fields))

    def _get_insert_sql(self):
        fields = []
        values = []
        cls = self.__class__

        for name, value in self.get_data().items():
            if name != "id":
                fields.append(name)
                if isinstance(value, str):
                    value = f'\'{value}\''
                values.append(value)

        values_str = [str(value) for value in values]
        sql = SQL_TEMPLATE["INSERT"].format(name=cls._get_name(),
                                            fields=", ".join(fields),
                                            values=", ".join(values_str)
                                            )
        return sql


class Node(ABC):
    @abstractmethod
    def visit(self, visitor: Visitor) -> None:
        pass

    def __and__(self, rhs):
        return Expression(self, " AND ", rhs)

    def __rand__(self, rhs):
        return Expression(rhs, " AND ", self)

    def __or__(self, rhs):
        return Expression(self, " OR ", rhs)

    def __ror__(self, rhs):
        return Expression(rhs, " OR ", self)

    def __eq__(self, rhs):
        return Expression(self, " = ", rhs)

    def __ne__(self, rhs):
        return Expression(self, " != ", rhs)

    def __lt__(self, rhs):
        return Expression(self, " < ", rhs)

    def __gt__(self, rhs):
        return Expression(self, " > ", rhs)

    def __le__(self, rhs):
        return Expression(self, " <= ", rhs)

    def __ge__(self, rhs):
        return Expression(self, " >= ", rhs)

    def in_(self, rhs):
        rhs = ', '.join(rhs)
        rhs = f'({rhs})'
        return Expression(self, " IN ", rhs)

    def like(self, rhs):
        return Expression(self, " LIKE ", rhs)


class BaseField(Node):
    def __init__(self, unique=False, null=False, default=None):
        self.unique = unique
        self.null = null
        self.default = default
        self.name = None
        self.model = None

    def get_settings(self):
        return self.__dict__

    def visit(self, visitor: Visitor) -> None:
        visitor.visit_field(self)

    def bind(self, model, name):
        self.model = model
        self.name = name

    def __repr__(self):
        return f"{self.model.__name__}.{self.name}"


class IntegerField(BaseField):
    type = "INTEGER"


class TextField(BaseField):
    type = "TEXT"


class RealField(BaseField):
    type = "REAL"


class BlobField(BaseField):
    type = "BLOB"


class Expression(Node):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.sql = ""
        if isinstance(rhs, Expression):
            self.rhs = rhs
        else:
            self.rhs = Value(rhs)

    def visit(self, visitor: Visitor) -> None:
        visitor.visit_expr(self)


class Value(Node):
    def __init__(self, value):
        self.val = value

    def visit(self, visitor: Visitor) -> None:
        visitor.visit_value(self)


class Visitor(ABC):
    @abstractmethod
    def visit_expr(self, element: Expression) -> None:
        pass

    @abstractmethod
    def visit_field(self, element: BaseField) -> None:
        pass

    @abstractmethod
    def visit_value(self, element: Value) -> None:
        pass


class SqlVisitor(Visitor):
    def __init__(self):
        self.sql = ""

    def visit_expr(self, element) -> None:
        self.sql += '('
        element.lhs.visit(self)
        self.sql += element.op
        element.rhs.visit(self)
        self.sql += ')'

    def visit_field(self, element) -> None:
        self.sql += str(element)

    def visit_value(self, element) -> None:
        self.sql += str(element.val)


class Select:
    def __init__(self, db, model):
        self.db = db
        self.model = model
        self.sql = ""

    def all(self):
        result = []
        fields = self.model._get_fields()
        fields_name = ["id"]
        for field in fields:
            fields_name.append(field[0])

        self.sql = SQL_TEMPLATE["SELECT_ALL"].format(name=self.model._get_name())
        for row in self.db._execute(self.sql).fetchall():
            data = dict(zip(fields_name, row))
            result.append(self.model(**data))
        return result


db = SqliteDatabase()
db.connect("newdb.db")


class Author(Table):
    name = TextField()


humans = Author.objects.all()
for x in humans:
    print(x.name)