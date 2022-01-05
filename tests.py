import unittest
from pivorm import *
import os


class TestDatabaseCreation(unittest.TestCase):
    def test_it(self):
        global db
        db = SqliteDatabase()
        db1 = SqliteDatabase()
        assert (db == db1)


class TestDatabaseConnection(TestDatabaseCreation):
    def test_it(self):
        super().test_it()
        if os.path.exists("test.db"):
            os.remove("test.db")
        db.connect("test.db")
        with self.assertRaises(Exception) as context:
            db.connect("second.db")
        self.assertTrue("connection is already open" in str(context.exception))


class TestModelDefine(TestDatabaseCreation):
    def test_it(self):
        global Parent, Child

        class Parent(Table):
            name = TextField()
            age = IntegerField()
            weight = RealField()

        class Child(Table):
            name = TextField()
            age = IntegerField()
            weight = RealField()
            parent = ForeignKey(Parent)

        assert(Parent.name.type == "TEXT")
        assert(Child.parent.table == Parent)


class TestTableCreation(TestModelDefine):
    def test_it(self):
        super().test_it()
        Parent.create()
        Child.create()

        assert (Parent._get_create_sql() == "CREATE TABLE IF NOT EXISTS parent "
                                            "(id INTEGER PRIMARY KEY, age INTEGER NOT NULL, name TEXT NOT NULL, "
                                            "weight REAL NOT NULL)")
        assert(Child._get_create_sql() == "CREATE TABLE IF NOT EXISTS child (id INTEGER PRIMARY KEY, "
                                          "age INTEGER NOT NULL, "
                                          "name TEXT NOT NULL, parent_id INTEGER, weight REAL NOT NULL, "
                                          "FOREIGN KEY (parent_id) REFERENCES parent(id))")


class TestModelInstanceDefine(TestTableCreation):
    def test_it(self):
        super().test_it()
        global parent1, parent2, child1, child2, child3

        parent1 = Parent(name="parent1", age=34, weight=90.1)
        parent2 = Parent(name="parent2", age=41, weight=85.6)
        child1 = Child(name="child1", age=10, weight=35, parent=parent1)
        child2 = Child(name="child2", age=12, weight=36.5, parent=parent2)
        child3 = Child(name="child3", age=8, weight=30, parent=parent2)

        assert (parent1.name == "parent1")
        assert (child1.age == 10)
        assert (child1.parent == parent1)


class TestModelInstanceCreation(TestModelInstanceDefine):
    def test_it(self):
        super().test_it()

        parent1.save()
        parent2.save()
        child1.save()
        child2.save()
        child3.save()

        assert(child3._get_insert_sql() == "INSERT INTO child (name, age, weight, parent_id) VALUES ('child3', 8, 30, 2)")
        assert(parent1.id == 1)
        assert(child3.id == 3)


class TestSelectAll(TestModelInstanceDefine):
    def test_it(self):
        super().test_it()
        global all_parents, all_children
        all_parents = Parent.objects.all()
        all_children = Child.objects.all()
        assert ([x.name for x in all_parents] == ["parent1", "parent2"])
        assert ([x.name for x in all_children] == ["child1", "child2", "child3"])


class TestSelectFilter(TestSelectAll):
    def test_it(self):
        super().test_it()
        par1_child = all_children.filter(Parent.name == 'parent1')
        par2_child = all_children.filter(Parent.name == 'parent2')
        assert ([x.name for x in par1_child] == ["child1"])
        assert ([x.name for x in par2_child] == ["child2", "child3"])


class TestSelectFilter2(TestSelectAll):
    def test_it(self):
        super().test_it()
        all_parents_like = Parent.objects.filter(Parent.name.like('par%') & (Parent.id == 1))
        assert ([x.name for x in all_parents_like] == ["parent1"])


class TestSelectGet(TestSelectAll):
    def test_it(self):
        super().test_it()
        child = Child.objects.get(Child.name == 'child2')
        assert(child.name == "child2")
        assert(child.parent.name == parent2.name)


class TestSelectAllWithResult(TestSelectAll):
    def test_it(self):
        super().test_it()
        all_parents2 = all_parents.all()
        assert ([x.name for x in all_parents2] == [x.name for x in all_parents])


if __name__ == '__main__':
    unittest.main()
