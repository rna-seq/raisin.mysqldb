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
                # Database connection may have been closed already
                try:
                    database.conn.close()
                except ProgrammingError:
                    pass
atexit.register(close_database_connection)


class DB:
    """
    Encapsulate a MySQL connection in a class instance
    """

    conn = None

    def __init__(self, database, connection):
        """Register the database."""
        self.database = database
        self.connection = connection

        # Register database so it can be closed upon exit
        DBS[self.database] = self

    def connect(self):
        """Connect to the MySQL database"""
        try:
            self.conn = MySQLdb.connect(host=self.connection['server'],
                                        port=int(self.connection['port']),
                                        user=self.connection['user'],
                                        passwd=self.connection['password'],
                                        db=self.database)
        except:
            import sys
            exc_info = sys.exc_info()
            print exc_info 
            LOG.exception("Can't establish connection to %s (%s)" % (self.database, exc_info))

    def query(self, sql, args=None):
        """Query the MySQL database"""
        if self.conn is None:
            LOG.debug("Connect because the connection is not yet existing")
            # Not yet connected to the database
            self.connect()

        if self.conn is None:
            raise MySQLdb.OperationalError

        cursor = self.conn.cursor()

        try:
            cursor.execute(sql, args)
        except (AttributeError, MySQLdb.OperationalError):
            LOG.exception("MySQLdb.OperationalError")
            self.connect()
            if self.conn is None:
                raise
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, args)
            except Exception:
                LOG.exception("Execution failed, trying again.")
                try:
                    cursor.execute(sql, args)
                except MySQLdb.OperationalError, err:
                    error_type = _get_error_type(err)
                    # SERVER_LOST error
                    if error_type == 2013:
                        LOG.exception("SERVER_LOST while retrying execution")
                    else:
                        LOG.exception("Retrying execution failed")
                    raise
        except ProgrammingError, err:
            error_type = _get_error_type(err)
            # SQL syntax error
            if error_type == 1064:
                LOG.debug("""Execution failed: %s""" % sql)
            else:
                LOG.exception("Execution failed")
            raise
        except:
            raise
        return cursor


def run_method_using_mysqldb(method, dbs, confs, marker):
    """
    Run a method containing some code making use of the MySQL database.
    If anything goes wrong, return the marker, otherwise the result.
    """
    try:
        data = method(dbs, confs)
    except ProgrammingError, err:
        error_type = _get_error_type(err)
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


def _get_error_type(err):
    """Get the error type from the exception in a safe way"""
    if hasattr(err, 'args'):
        try:
            error_type = err.args[0]
        except IndexError:
            error_type = None
    return error_type
