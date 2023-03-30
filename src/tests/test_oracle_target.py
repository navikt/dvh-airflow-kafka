from typing import Any, Dict, Text
import pytest
import os

from unittest import mock
import yaml


import oracledb



# sample subclassed Connection which overrides the constructor (so no
# parameters are required) and the cursor() method (so that the subclassed
# cursor is returned instead of the default cursor implementation)
class StubbedConnection():

    def __init__(self):
        print("CONNECTION")
        
    def cursor(self):
        return Cursor()


# sample subclassed cursor which overrides the execute() and fetchone()
# methods in order to perform some simple logging
class Cursor():

    def execute(self, statement, args):
        print("EXECUTE", statement)
        print("ARGS:")
        for arg_index, arg in enumerate(args):
            print("   ", arg_index + 1, "=>", repr(arg))
        # return super().execute(statement, args)

    def fetchone(self):
        print("FETCHONE")
        return 1
        # return super().fetchone()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        pass



@pytest.fixture(autouse=True)
def mock_settings_env_vars( ):
    pass

@pytest.mark.unit
def test_connection(test_config):
























if __name__ == '__main__':
    print('Tester stub')
    connection = StubbedConnection()

    with connection.cursor() as cursor:

        # demonstrate that the subclassed connection and cursor are being used
        cursor.execute("select count(*) from ChildTable where ParentId = :1", (30,))
        count = cursor.fetchone()
        print("COUNT:", int(count))