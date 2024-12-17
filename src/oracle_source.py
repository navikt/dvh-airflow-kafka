import os
from typing import Dict, Text, Any, List, Tuple
import oracledb
from base import Target
from transform import int_ms_to_date
import json
import logging
import oracledb


# sample subclassed Connection which overrides the constructor (so no
# parameters are required) and the cursor() method (so that the subclassed
# cursor is returned instead of the default cursor implementation)
class OracleSource(oracledb.Connection):

    oracledb.init_oracle_client()

    # connection_class = oracledb.connect

    def __init__(self, config):
        pass
