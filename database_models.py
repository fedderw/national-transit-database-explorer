from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.types import Enum
import pandas as pd
import yaml

# Load the configurations from YAML file
with open("conf/main.yaml") as file:
    conf = yaml.safe_load(file)

mode_values = list(conf["MODE_MAPPING"].values()) + [
    "Unknown",
]
tos_values = list(conf["TOS_MAPPING"].values()) + [
    "Unknown",
]

# Define the SQLAlchemy base
Base = declarative_base()


# Define the ORM class for each dimension table
class Agency(Base):
    __tablename__ = "agency"
    ntd_id = Column(Integer, primary_key=True)
    agency_name = Column(String)


class Status(Base):
    __tablename__ = "status"
    status_id = Column(Integer, primary_key=True)
    status = Column(Enum(*conf["STATUS"]))


class ReporterType(Base):
    __tablename__ = "reporter_type"
    reporter_id = Column(Integer, primary_key=True)
    reporter_type = Column(Enum(*conf["REPORTER_TYPE"]))


class UrbanizedArea(Base):
    __tablename__ = "urbanized_area"
    uace_cd = Column(Integer, primary_key=True)
    uza_name = Column(String)


class Mode(Base):
    __tablename__ = "mode"
    mode_id = Column(Integer, primary_key=True)
    mode = Column(Enum(*mode_values))


class TypeOfService(Base):
    __tablename__ = "type_of_service"
    tos_id = Column(Integer, primary_key=True)
    tos = Column(Enum(*tos_values))


# Define the fact table
class AgencyModeMonth(Base):
    __tablename__ = "AgencyModeMonth"
    ntd_id = Column(Integer, ForeignKey("agency.ntd_id"), primary_key=True)
    status_id = Column(
        Integer, ForeignKey("status.status_id"), primary_key=True
    )
    reporter_id = Column(
        Integer, ForeignKey("reporter_type.reporter_id"), primary_key=True
    )
    uace_cd = Column(
        Integer, ForeignKey("urbanized_area.uace_cd"), primary_key=True
    )
    mode_id = Column(Integer, ForeignKey("mode.mode_id"), primary_key=True)
    tos_id = Column(
        Integer, ForeignKey("type_of_service.tos_id"), primary_key=True
    )
    month = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    UPT = Column(Float)
    VRM = Column(Float)
    VRH = Column(Float)
    VOMS = Column(Float)
    # Relationship definitions for foreign keys
    agency = relationship("Agency")
    status = relationship("Status")
    reporter_type = relationship("ReporterType")
    urbanized_area = relationship("UrbanizedArea")
    mode = relationship("Mode")
    type_of_service = relationship("TypeOfService")
