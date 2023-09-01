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

class TeamGameStats(Base):
    __tablename__='team_game_stats'
    id = Column(Integer, primary_key=True)
    
    home = Column(Boolean)
    total = Column(Float)
    off_total = Column(Float)
    off_pass = Column(Float)
    off_rush = Column(Float)
    off_exp_turn = Column(Float)
    
    def_tot = Column(Float)
    def_pass = Column(Float)
    def_rush = Column(Float)
    def_exp_turn = Column(Float)
        
    first_downs = Column(Integer)
    rush_att = Column(Integer)
    rush_tds = Column(Integer)
    rush_yds = Column(Integer)
    
    pass_att = Column(Integer)
    pass_comp = Column(Integer)
    pass_yds = Column(Integer)
    pass_tds = Column(Integer)
    pass_int = Column(Integer)
    
    sack_yds = Column(Integer)
    total_yds = Column(Integer)
    fumbles = Column(Integer)
    fumbles_lost = Column(Integer)
    turnovers = Column(Integer)
    penalties = Column(Integer)
    penalty_yds = Column(Integer)
    
    time_of_pos = Column(Interval)
    
    game = relationship("Game", back_populates="team_stats")

class Game(Base):
    __tablename__= 'game'
    id = Column(Integer, primary_key=True)
    
    away_team = Column(String)
    away_score = Column(Integer)
    away_team_stats_id = Column(Integer, ForeignKey('team_game_stats.id'))
    away_team_stats = relationship("TeamGameStats", back_populates="game")  
    
    home_team = Column(String)
    home_score = Column(Integer)
    home_team_stats_id = Column(Integer, ForeignKey('team_game_stats.id'))
    home_team_stats = relationship("TeamGameStats", back_populates="game")  

    roof = Column(String)
    surface = Column(String)
    Weather = Column(String)
    
    over_under = Column(Float)   

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
    cls.age = Column(Float)
    cls.away_game = Column(Boolean)

    cls.bye = Column(Boolean, default=False)
    cls.inactive = Column(Boolean, default=False)
    cls.dnp = Column(Boolean, default=False)
    cls.player_suspended = Column(Boolean, default=False)
    
    cls.game_id = Column(Integer, ForeignKey('game.id'))
    cls.game = relationship("Game", back_populates="game_stats")

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

    name = Column(String)
    dob = Column(DateTime)
    height_cm = Column(Integer)
    weight_kg = Column(Integer)

    drafted_year = Column(DateTime)
    drafted_overall = Column(Integer)

    pos = Column(String)
    college = Column(String)


class QbPlayer(Player):
    __tablename__ = 'qb_player'
    id = Column(Integer, ForeignKey('players.id'), primary_key=True)
    
    __mapper_args__ = {
        'polymorphic_identity': 'qb_player',
        'inherit_condition': (id == Player.id),
    }
    
    single_stats = relationship("QbGameStats", back_populates="qb_player")

class QbGameStats(Base):
    __tablename__ = 'qb_game_stats'
    id = Column(Integer, primary_key=True)
    
    player = relationship("QbPlayer", back_populates="qb_game_stats")

add_pass_stat_cols(QbGameStats)
add_rush_stat_cols(QbGameStats)
add_game_stats(QbGameStats)
    
class SkillPlayer(Player):
    __tablename__ = 'skill_player'
    id = Column(Integer, ForeignKey('players.id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'skill_player',
        'inherit_condition': (id == Player.id),
    }

    single_stats = relationship("SkillGameStats", back_populates="qb_player")

class SkillGameStats(Base):
    __tablename__ = 'skill_game_stats'
    id = Column(Integer, primary_key=True)
    
    player_id = Column(Integer, ForeignKey('skill_player.id'))
    player = relationship("SkillPlayer", back_populates="skill_game_stats")

add_rec_stat_cols(SkillGameStats)
add_rush_stat_cols(SkillGameStats)
add_game_stats(SkillGameStats)