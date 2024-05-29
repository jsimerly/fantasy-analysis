from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Interval, Boolean, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

load_dotenv()
Base = declarative_base()

#Models

class Player(Base):
    __tablename__ = 'players'

    id = Column(Integer, primary_key=True)
    gsis_id = Column(String)
    gsis_it_id = Column(String)
    ktc_id = Column(String)
    sleeper_id = Column(String)
    rotoworld_id = Column(String)
    rotowire_id = Column(String)
    yahoo_id = Column(String)
    espn_id = Column(String)
    swish_id = Column(String)

    status = Column(String) ## Choice
    display_name = Column(String)
    football_name = Column(String)
    birth_date = Column(DateTime)

    position_group = Column(String) ## Choice
    position = Column(String) ## Choice

    height = Column(Float)
    weight = Column(Float)
    yoe = Column(Integer)
    team_abbr = Column(String)
    current_team_id= Column(Integer)

    entry_year = Column(Integer)
    rookie_year = Column(Integer)

    college = Column(String)
    college_conf = Column(String)
    draft_club = Column(String)
    draft_number = Column(Integer)
    draft_round = Column(Integer)

    uniform_number = Column(String)
    jersey_number = Column(Integer)

    game_stats = relationship("GameStats", back_populates="player")
    ktc_values = relationship("KtcValue", back_populates="player")

    last_updated = Column(DateTime, onupdate=func.now(), default=func.now())

class GameStats(Base):
    __tablename__ = 'game_stats'
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'))

    season = Column(Integer)
    week = Column(Integer)
    season_type = Column(String)
    opponent_team = Column(String)

    completions = Column(Integer)
    attempts = Column(Integer)
    passing_yards = Column(Integer)
    passing_tds = Column(Integer)
    interceptions = Column(Integer)
    sacks = Column(Integer)
    sack_yards = Column(Integer)
    sack_fumbles = Column(Integer)
    sack_fumbles_lost = Column(Integer)
    passing_air_yards = Column(Integer)
    passing_yards_after_catch = Column(Integer)
    passing_first_downs = Column(Integer)
    passing_epa = Column(Float)

    rushing_tds = Column(Integer)
    rushing_fumbles = Column(Integer)
    rushing_fumbles_lost = Column(Integer)
    rushing_first_downs = Column(Integer)
    rushing_epa = Column(Float)
    rushing_2pt_conversion = Column(Integer)

    receptions = Column(Integer)
    targets = Column(Integer)
    receiving_yards = Column(Integer)
    receiving_tds = Column(Integer)
    receiving_fumbles = Column(Integer)
    receiving_fumbles_lost = Column(Integer)
    receiving_air_yards = Column(Integer)
    receiving_yards_after_catch = Column(Integer)
    receiving_first_downs = Column(Integer)
    receiving_epa = Column(Float)
    receiving_2pt_conversions = Column(Integer)
    racr = Column(Float)
    target_share = Column(Float)
    air_yards_share = Column(Float)
    wopr = Column(Float)
    special_teams_tds = Column(Integer)

    fantasy_points = Column(Float)
    fantasy_points_ppr = Column(Float)

    last_updated = Column(DateTime, onupdate=func.now(), default=func.now())

    player = relationship("Player", back_populates="game_stats")

class KtcValue(Base):
    __tablename__ = 'ktc_values'
    player_id = Column(Integer, ForeignKey('players.id'), primary_key=True)
    date = Column(DateTime, primary_key=True)

    value = Column(SmallInteger)
    player = relationship("Player", back_populates="ktc_values")


if __name__ == '__main__':
    DB_USERNAME = os.environ['DB_USERNAME']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_HOST = os.environ['DB_HOST']
    DB_PORT = os.environ['DB_PORT']
    DB_NAME = os.environ['DB_NAME']

    DB_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DB_URL, echo=True)

    Base.metadata.create_all(engine)