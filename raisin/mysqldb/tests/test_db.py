import sys
import unittest
from raisin.mysqldb import DB
from raisin.mysqldb import DBS
from raisin.mysqldb import close_database_connection
from raisin.mysqldb import run_method_using_mysqldb
from MySQLdb.cursors import ProgrammingError
from MySQLdb.cursors import OperationalError

def get_dummy_db():
    db = None
    host = None
    port = None
    user = None
    passwd = None
    return DB(db, host, port, user, passwd)

class DBTest(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.dummy_db = get_dummy_db()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        close_database_connection()

    def test_dummy_db_has_the_attributes(self):
        self.failUnless(self.dummy_db.db == None)
        self.failUnless(self.dummy_db.host == None)
        self.failUnless(self.dummy_db.port == None)
        self.failUnless(self.dummy_db.user == None)
        self.failUnless(self.dummy_db.passwd == None)

    def test_dummy_db_does_get_added_to_the_registry(self):
        self.failUnless(DBS[None] == self.dummy_db)

    def test_running_method_using_mysqldb(self):
        def dummy_method():
            return "Some result data from the MySQL database"
        method = dummy_method
        parameters = {}
        marker = None
        result = run_method_using_mysqldb(dummy_method, parameters, marker)
        self.failUnless(result == "Some result data from the MySQL database")

    def test_running_method_using_mysqldb_with_programmingerror(self):
        def dummy_method():
            raise ProgrammingError
        method = dummy_method
        parameters = {}
        marker = None
        result = run_method_using_mysqldb(dummy_method, parameters, marker)
        self.failUnless(result == marker)

    def test_running_method_using_mysqldb_with_operationalerror(self):
        def dummy_method():
            raise OperationalError
        method = dummy_method
        parameters = {}
        marker = None
        result = run_method_using_mysqldb(dummy_method, parameters, marker)
        self.failUnless(result == marker)

    def test_running_method_using_mysqldb_with_attributerror(self):
        def dummy_method():
            raise AttributeError
        method = dummy_method
        parameters = {}
        marker = None
        cla, exc, trbk = None, None, None
        try:
            run_method_using_mysqldb(dummy_method, parameters, marker)
        except Exception, e:
            cla, exc, trbk = sys.exc_info()
        self.failUnless(cla == AttributeError)

# make the test suite.
def suite():
    loader = unittest.TestLoader()
    testsuite = loader.loadTestsFromTestCase(DBTest)
    return testsuite

# Make the test suite; run the tests.
def test_main():
    testsuite = suite()
    runner = unittest.TextTestRunner(sys.stdout, verbosity=2)
    result = runner.run(testsuite)

if __name__ == "__main__":
    test_main()