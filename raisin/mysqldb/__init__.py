import sys
import traceback
import atexit
import MySQLdb
from MySQLdb.cursors import ProgrammingError
from MySQLdb.cursors import OperationalError

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
            print traceback.format_exc()
            print "Can't establish connection"
            
    def query(self, sql):        
        #print "Query SQL database"
        if self.conn is None:
            print "Connect because the connection is not yet existing"
            # Not yet connected to the database
            self.connect()

        if self.conn is None:
            raise MySQLdb.OperationalError

        # XXX It may be better to use a specialized library for managing the MySQL connections
        #
        # Some candidates: PySQLPool, OurSQL, myconnpy
        #print "Query database: %s" % sql

        cursor = self.conn.cursor()

        try:
            cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            print traceback.format_exc()
            print "MySQLdb.OperationalError"
            self.connect()
            if self.conn is None:
                raise
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)
            except:
                print traceback.format_exc()
                print "Retry Execution"
                self._executeRetry(self.conn, cursor, sql)
        except ProgrammingError, e:
            if e.args[0] == 1064: # SQL syntax error
                print sql
            raise
        except:
            raise
        return cursor

    def _executeRetry(self, conn, cursor, query):
        while 1:
            try:
                return cursor.execute(query)
            except MySQLdb.OperationalError, e:                
                print traceback.format_exc()
                if e.args[0] == 2013: # SERVER_LOST error
                    print conn, str(e), 'ERROR'
                else:
                    raise

def run_method_using_mysqldb(method, dbs, confs, marker):
    """
    Run a method containing some code making use of the MySQL database.
    If anything goes wrong, return the marker, otherwise the result.
    """
    try:
        data = method(dbs, confs)
    except ProgrammingError, e:
        if e.args[0] == 1146: # Table does not exist
            print "ProgrammingError: %s" % e
        else:
            print method, dbs, confs
            print traceback.format_exc()
        return marker
    except OperationalError:
        print traceback.format_exc()
        return marker
    except:
        print traceback.format_exc()
        return marker
    return data        
        
