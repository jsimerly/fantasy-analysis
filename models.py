import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Interval, Boolean
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, relationship
import alembic
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Read environment variables
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_NAME = os.environ['DB_NAME']

# Construct the database URL
DB_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, echo=True)

#Create Models
Base = declarative_base()


def add_pass_stat_cols(cls):
    cls.pass_comp = Column(Integer)
    cls.pass_att = Column(Integer)
    cls.pass_yds = Column(Integer)
    cls.pass_tds = Column(Integer)
    cls.pass_ints = Column(Integer)
    
    # cls.pass_first_downs = Column(Integer)
    # cls.pass_first_down_perc = Column(Float)
    # cls.pass_IAY = Column(Integer)
    # cls.pass_CAY = Column(Integer)
    # cls.pass_drops = Column(Integer)
    # cls.pass_bad_throw = Column(Integer)
    # cls.pass_bad_throw_perc = Column(Float)

    # cls.pass_pressure_perc = Column(Float)
    cls.pass_sacks = Column(Integer)
    cls.pass_per_att = Column(Integer)
    cls.pass_adj_per_att = Column(Integer)

def add_rush_stat_cols(cls):
    cls.rush_first_downs = Column(Integer)
    cls.rush_ybc = Column(Integer)
    cls.rush_yac = Column(Integer)
    cls.rush_brkTkl = Column(Integer)
    
    cls.rush_att = Column(Integer)
    cls.rush_yds = Column(Integer)
    cls.rush_per_att = Column(Float)
    cls.rush_tds = Column(Integer)  

def add_rec_stat_cols(cls):
    cls.rec_first_downs = Column(Integer)
    cls.rec_ybc = Column(Integer)
    cls.rec_yac = Column(Integer)
    cls.rec_adot = Column(Float)
    cls.rec_brkTkl = Column(Integer)
    cls.rec_drop = Column(Integer)
    cls.rec_int = Column(Integer)
    
    cls.rec_tgt = Column(Integer)
    cls.rec_rec = Column(Integer)
    cls.rec_yds = Column(Integer)
    cls.rec_per_rec = Column(Integer)
    cls.rec_tds = Column(Integer) 

def add_game_stats(cls):
    cls.team = Column(String)
    cls.opp = Column(String)

    cls.age = Column(Float)
    cls.away_game = Column(Boolean)

    cls.bye = Column(Boolean, default=False)
    cls.inactive = Column(Boolean, default=False)
    cls.dnp = Column(Boolean, default=False)
    cls.suspended = Column(Boolean, default=False)
    cls.ir = Column(Boolean, default=False)
    
    # cls.game_id = Column(Integer, ForeignKey('game.id'))
    # cls.game = relationship("Game", back_populates="game_stats")

    cls.year = Column(Integer)
    cls.week = Column(Integer)
    cls.date = Column(DateTime)

    cls.started = Column(Boolean)
    cls.playoff = Column(Boolean, default=False)

    cls.fumbles = Column(Integer)
    cls.snaps = Column(Integer)
    cls.snap_perc = Column(Float)
    
class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    pfr_id = Column(String, unique=True)
    current_team = Column(String)

    name = Column(String)
    dob = Column(DateTime)
    height_cm = Column(Integer)
    weight_kg = Column(Integer)

    drafted_year = Column(Integer)
    drafted_overall = Column(Integer)

    pos = Column(String)
    college = Column(String)

    qb_game_stats = relationship("QbGameStats", back_populates="player")
    skill_game_stats = relationship("SkillGameStats", back_populates="player")

class QbGameStats(Base):
    __tablename__ = 'qb_game_stats'
    id = Column(Integer, primary_key=True)
    
    player_id = Column(Integer, ForeignKey('players.id'))
    player = relationship("Player", back_populates="qb_game_stats")

add_pass_stat_cols(QbGameStats)
add_rush_stat_cols(QbGameStats)
add_game_stats(QbGameStats)
    
class SkillGameStats(Base):
    __tablename__ = 'skill_game_stats'
    id = Column(Integer, primary_key=True)
    
    player_id = Column(Integer, ForeignKey('players.id'))
    player = relationship("Player", back_populates="skill_game_stats")

add_rec_stat_cols(SkillGameStats)
add_rush_stat_cols(SkillGameStats)
add_game_stats(SkillGameStats)