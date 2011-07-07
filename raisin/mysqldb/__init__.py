"""
raisin.mysqldb

Implements a wrapper for MySQLdb that takes special care about exceptions.

Contains a class for connecting and executing SQL. Has a method for executing 
Python methods containing calls to SQL, which hides the implementation details
of MySQLdb, and especially avoids having to care about the exceptions. 
Finally, the databases connections are boing closed when exiting.
"""
import logging
import sys
import traceback
import atexit
import MySQLdb
from MySQLdb.cursors import ProgrammingError
from MySQLdb.cursors import OperationalError

log = logging.getLogger(__name__)

DBS = {}

# Play nice and close the database connection when exiting    
def close_database_connection():
    for key, db in DBS.items():
        if not db is None:
            if not db.conn is None:
                db.conn.close()
                    
atexit.register(close_database_connection)    

class DB:
    """
    Encapsulate a MySQL connection in a class instance
    """

    conn = None

    def __init__(self, db, host, port, user, passwd):
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        
        # Register db so it can be closed upon exit
        DBS[db] = self
        
    def connect(self):
        try:
            self.conn = MySQLdb.connect(host = self.host, 
                                        port=self.port, 
                                        user=self.user, 
                                        passwd=self.passwd, 
                                        db=self.db)
        except:
            log.exception("Can't establish connection")
            
    def query(self, sql):        
        if self.conn is None:
            log.debug("Connect because the connection is not yet existing")
            # Not yet connected to the database
            self.connect()

        if self.conn is None:
            raise MySQLdb.OperationalError

        cursor = self.conn.cursor()

        try:
            cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            log.exception("MySQLdb.OperationalError")
            self.connect()
            if self.conn is None:
                raise
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)
            except:
                log.exception("Execution failed")
                self._executeRetry(self.conn, cursor, sql)
        except ProgrammingError, e:
            if e.args[0] == 1064: # SQL syntax error
                log.debug("""Execution failed: %s""" % sql)
            else:
                log.exception("Execution failed")
            raise
        except:
            raise
        return cursor

    def _executeRetry(self, conn, cursor, query):
        while 1:
            try:
                return cursor.execute(query)
            except MySQLdb.OperationalError, e:                
                if e.args[0] == 2013: # SERVER_LOST error
                    log.exception("SERVER_LOST error while retrying execution")
                else:
                    log.exception("Retrying execution failed")
                    raise

def run_method_using_mysqldb(method, dbs, confs, marker):
    """
    Run a method containing some code making use of the MySQL database.
    If anything goes wrong, return the marker, otherwise the result.
    """
    try:
        data = method(dbs, confs)
    except ProgrammingError, e:
        error_type = None
        try:
            error_type = e.args[0]
        except:
            pass
        if error_type == 1146: # Table does not exist
            log.exception("ProgrammingError")
        else:
            log.exception("""Error %s""" % error_type)
        return marker
    except OperationalError:
        log.exception("OperationalError")
        return marker
    except:
        log.exception("Error")
        return marker
    return data
        
