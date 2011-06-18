= big.mysqldb =

This module contains a class for keeping a connection to a MySQL database:

    class DB:
        """
        Encapsulate a MySQL connection in a class instance
        """

It also contains a method to run Python methods while raising the right Exceptions when
needed: 
    
    def run_method_using_mysqldb(method, parameters, marker):
       """
        Run a method containing some code making use of the MySQL database.
        If anything goes wrong, return the marker, otherwise the result.
        """
        