"""Implements a wrapper for MySQLdb that takes special care about exceptions.

DB: A class for connecting and executing SQL.

run_method_using_mysqldb: A proxy method for methods calling MySQLDB.

    * hides the implementation details of MySQLdb

    * avoids having to care about the exceptions

Databases connections are boing closed when exiting using atexit.

"""
import logging
import atexit
import MySQLdb
from MySQLdb.cursors import ProgrammingError
from MySQLdb.cursors import OperationalError

LOG = logging.getLogger(__name__)

DBS = {}


def close_database_connection():
    """Play nice and close the database connection when exiting"""
    for database in DBS.values():
        if not database is None:
            if not database.conn is None:
                database.conn.close()

atexit.register(close_database_connection)


class DB:
    """
    Encapsulate a MySQL connection in a class instance
    """

    conn = None

    def __init__(self, database, host, port, user, passwd):
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd

        # Register database so it can be closed upon exit
        DBS[database] = self

    def connect(self):
        try:
            self.conn = MySQLdb.connect(host=self.host,
                                        port=self.port,
                                        user=self.user,
                                        passwd=self.passwd,
                                        db=self.database)
        except Exception:
            LOG.exception("Can't establish connection")

    def query(self, sql):
        if self.conn is None:
            LOG.debug("Connect because the connection is not yet existing")
            # Not yet connected to the database
            self.connect()

        if self.conn is None:
            raise MySQLdb.OperationalError

        cursor = self.conn.cursor()

        try:
            cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            LOG.exception("MySQLdb.OperationalError")
            self.connect()
            if self.conn is None:
                raise
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)
            except Exception:
                LOG.exception("Execution failed")
                self._executeRetry(self.conn, cursor, sql)
        except ProgrammingError, e:
            error_type = _get_error_type(e)
            # SQL syntax error
            if error_type == 1064:
                LOG.debug("""Execution failed: %s""" % sql)
            else:
                LOG.exception("Execution failed")
            raise
        except:
            raise
        return cursor

    def _executeRetry(self, conn, cursor, query):
        while 1:
            try:
                return cursor.execute(query)
            except MySQLdb.OperationalError, e:
                # SERVER_LOST error
                if e.args[0] == 2013:
                    LOG.exception("SERVER_LOST error while retrying execution")
                else:
                    LOG.exception("Retrying execution failed")
                    raise


def run_method_using_mysqldb(method, dbs, confs, marker):
    """
    Run a method containing some code making use of the MySQL database.
    If anything goes wrong, return the marker, otherwise the result.
    """
    try:
        data = method(dbs, confs)
    except ProgrammingError, e:
        error_type = _get_error_type(e)
        # Table does not exist
        if error_type == 1146:
            LOG.exception("ProgrammingError")
        else:
            LOG.exception("""Error %s""" % error_type)
        return marker
    except OperationalError:
        LOG.exception("OperationalError")
        return marker
    except:
        LOG.exception("Error")
        return marker
    return data


def _get_error_type(e):
    error_type = None
    if hasattr(e, 'args'):
        try:
            error_type = e.args[0]
        except IndexError:
            pass
    return error_type
