from __future__ import annotations
import sqlite3
import inspect
from abc import ABC, abstractmethod

SQL_TEMPLATE = {
    "CREATE": "CREATE TABLE IF NOT EXISTS {table} ({fields})",
    "INSERT": "INSERT INTO {name} ({fields}) VALUES ({values})",
    "SELECT_ALL": "SELECT {name}.* FROM {name}",
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
        else:
            raise Exception(f"connection is already open")

    def _execute(self, sql):
        return self.cursor.execute(sql)

    def close(self):
        if self.connection is not None:
            self.cursor.close()
            self.connection.close()
            self.cursor = None
            self.connection = None


class Table:
    def __init__(self, **kwargs):
        self._data = {
            "id": None
        }
        for k, v in kwargs.items():
            self._data[k] = v

    def __init_subclass__(cls, **kwargs):
        cls.id = IntegerField()
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
            if isinstance(field, BaseField) and name != 'id':
                field_row = [name, field.type]
                settings = field.get_settings()
                if settings['unique']:
                    field_row.append("UNIQUE")
                if not settings['null']:
                    field_row.append("NOT NULL")
                if settings['default'] is not None:
                    field_row.append("DEFAULT {value}".format(value=settings['default']))
                fields.append(field_row)
            elif isinstance(field, ForeignKey):
                name = name+"_id"
                fields.append([name, "INTEGER"])
        return fields

    @classmethod
    def _bind_fields(cls):
        for name, field in inspect.getmembers(cls):
            if isinstance(field, BaseField):
                field.bind(cls, name)

    @classmethod
    def _get_create_sql(cls):
        fields = [["id", "INTEGER PRIMARY KEY"], *cls._get_fields()]
        for name, field in inspect.getmembers(cls):
            if isinstance(field, ForeignKey):
                fields.append([f"FOREIGN KEY ({name}) REFERENCES {field.table._get_name()}(id)"])
        fields = [" ".join(field) for field in fields]

        return SQL_TEMPLATE["CREATE"].format(table=cls._get_name(),
                                             fields=", ".join(fields))

    def _get_insert_sql(self):
        fields = []
        values = []
        cls = self.__class__

        for name, value in self.get_data().items():
            if isinstance(value, Table):
                fields.append(name+"_id")
                values.append(value.id)
            else:
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

    @classmethod
    def create(cls):
        return cls.db._execute(cls._get_create_sql())

    def save(self):
        sql = self._get_insert_sql()
        cursor = self.db._execute(sql)
        self._data["id"] = cursor.lastrowid
        print(cursor.lastrowid)
        self.db.connection.commit()


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
        self.type = None
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


class ForeignKey:
    def __init__(self, table):
        self.table = table


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
        if isinstance(element.val, str):
            element.val = f'\'{element.val}\''
        self.sql += str(element.val)


class Select:
    def __init__(self, db, model, sql="", result=None, where=""):
        self.db = db
        self.model = model
        self.sql = sql
        self.where = where
        self.result = result

    def all(self):
        if self.result:
            return Select(self.db, self.model, self.sql, self.result, self.where)

        result = []
        fields = self.model._get_fields()
        fields_name = ["id"]
        for field in fields:
            fields_name.append(field[0])

        sql = SQL_TEMPLATE["SELECT_ALL"].format(name=self.model._get_name())
        for row in self.db._execute(sql).fetchall():
            new_fields_name, values = self.get_fk_table(fields_name, row)
            data = dict(zip(new_fields_name, values))
            result.append(self.model(**data))
        if not result:
            return None
        return Select(self.db, self.model, sql, result)

    def filter(self, expression):
        result = []
        fields = self.model._get_fields()
        fields_name = ["id"]
        for field in fields:
            fields_name.append(field[0])

        sql = SQL_TEMPLATE["SELECT_ALL"].format(name=self.model._get_name())
        expr_visitor = SqlVisitor()
        expression.visit(expr_visitor)
        where = self.where

        for name, field in inspect.getmembers(self.model):
            if isinstance(field, ForeignKey):
                sql += f" LEFT JOIN {name} ON {self.model._get_name()}.{name}_id = {name}.id"

        if not where:
            where = f" WHERE {expr_visitor.sql}"
        else:
            where += f" AND {expr_visitor.sql}"

        sql += where
        for row in self.db._execute(sql).fetchall():
            new_fields_name, values = self.get_fk_table(fields_name, row)
            data = dict(zip(new_fields_name, values))
            result.append(self.model(**data))
        if not result:
            return None
        return Select(self.db, self.model, sql, result, where)

    def get(self, expression):
        select = self.filter(expression)
        if not select.result:
            return None
        return select[0]

    def get_fk_table(self, fields, row):
        new_fields_name = []
        values = []
        for field, value in zip(fields, row):
            if field.endswith("_id"):
                field = field[:-3]
                fk = getattr(self.model, field)
                value = fk.table.objects.filter(fk.table.id == value)[0]
            new_fields_name.append(field)
            values.append(value)
        return new_fields_name, values

    def __iter__(self):
        return (i for i in self.result)

    def __getitem__(self, item):
        return self.result[item]


class Parent(Table):
    name = TextField()
    age = IntegerField(null=True)


class Child(Table):
    name = TextField()
    age = IntegerField(null=True)
    parent = ForeignKey(Parent)


class Test(Table):
    test = TextField()
