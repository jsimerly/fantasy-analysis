import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Interval, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import alembic
import psycopg2
from dotenv import load_dotenv
import os