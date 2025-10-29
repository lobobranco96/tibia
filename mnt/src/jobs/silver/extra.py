from src.job.utility import create_spark_session
from pyspark.sql import functions as F
from datetime import datetime
import logging
import sys


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

