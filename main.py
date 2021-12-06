from __future__ import annotations
import sqlite3
import inspect
from abc import ABC, abstractmethod


class SqliteDatabase:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

    def _execute(self, sql):
        return self.cursor.execute(sql)

    def create(self, table):
        return self._execute(table._get_create_sql())


class Table:
    def __init_subclass__(cls, **kwargs):
        cls._bind_fields()

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
        create_table_sql = "CREATE TABLE IF NOT EXISTS {table} ({fields})"
        fields = [["id", "INTEGER PRIMARY KEY"], *cls._get_fields()]
        fields = [" ".join(field) for field in fields]
        return create_table_sql.format(table=cls._get_name(),
                                       fields=", ".join(fields))


class Node(ABC):
    @abstractmethod
    def visit(self, visitor: Visitor) -> None:
        pass

    def __and__(self, rhs):
        return Expression(self, "AND", rhs)

    def __or__(self, rhs):
        return Expression(self, "OR", rhs)

    def __eq__(self, rhs):
        return Expression(self, "=", rhs)

    def __ne__(self, rhs):
        return Expression(self, "!=", rhs)

    def __lt__(self, rhs):
        return Expression(self, "<", rhs)

    def __gt__(self, rhs):
        return Expression(self, ">", rhs)

    def __le__(self, rhs):
        return Expression(self, "<=", rhs)

    def __ge__(self, rhs):
        return Expression(self, ">=", rhs)

    def in_(self, rhs):
        return Expression(self, "IN", rhs)

    def like(self, rhs):
        return Expression(self, "LIKE", rhs)


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


class Author(Table):
    name = TextField()


expr = (Author.name == 5) & (Author.name <= 10) | (Author.name > 15)
vis = SqlVisitor()
expr.visit(vis)
print(vis.sql)
