import configparser
import datetime
import logging
import os
from typing import List, Optional

from sqlalchemy import (
    ForeignKey, Integer, JSON, String, DateTime, Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column

from scrapy.utils.project import get_project_settings

__all__ = [
    "create_tables",
    "Country",
    "Daily",
    "Monthly",
    "System",
    "Weekly",
    "Yearly"
]

Base = declarative_base()

def create_tables(engine):
    Base.metadata.create_all(engine, checkfirst=True)


class Country(Base):
    __tablename__ = "country"

    pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sid: Mapped[int] = mapped_column(unique=True)
    name: Mapped[str] = mapped_column(nullable=False)
    systems: Mapped[Optional["System"]] = relationship(back_populates="country", uselist=True)


class System(Base): 
    __tablename__ = "system"

    pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sid: Mapped[int] = mapped_column(unique=True)
    id: Mapped[int] = mapped_column(nullable=True)
    name: Mapped[str] = mapped_column(nullable=True)
    latitude: Mapped[float] = mapped_column(nullable=True)
    longitude: Mapped[float] = mapped_column(nullable=True)
    number_of_panels: Mapped[int] = mapped_column(nullable=True)
    panel_max_power: Mapped[float] = mapped_column(nullable=True)
    size: Mapped[float] = mapped_column(nullable=True)
    panel_brand: Mapped[str] = mapped_column(nullable=True)
    orientation: Mapped[str] = mapped_column(nullable=True)
    number_of_inverters: Mapped[int] = mapped_column(nullable=True)
    inverter_brand: Mapped[str] = mapped_column(nullable=True)
    inverter_size: Mapped[float] = mapped_column(nullable=True)
    post_code: Mapped[str] = mapped_column(nullable=True)
    installation_date: Mapped[str] = mapped_column(nullable=True)
    shading: Mapped[str] = mapped_column(nullable=True)
    tilt: Mapped[str] = mapped_column(nullable=True)
    comments: Mapped[str] = mapped_column(nullable=True)
    daily: Mapped[Optional["Daily"]] = relationship(uselist=False, back_populates='system')
    weekly: Mapped[Optional["Weekly"]] = relationship(uselist=False, back_populates='system')
    monthly: Mapped[Optional["Monthly"]] = relationship(uselist=False, back_populates='system')
    yearly: Mapped[Optional["Yearly"]] = relationship(uselist=False, back_populates='system')
    country_sid: Mapped[Optional[int]] = mapped_column(ForeignKey("country.sid"))
    country: Mapped[Optional["Country"]] = relationship(back_populates="systems")


class Daily(Base):
    __tablename__ = "daily"

    pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sid: Mapped[int] = mapped_column(ForeignKey("system.sid"))
    system: Mapped[Optional["System"]] = relationship(back_populates="daily")
    date: Mapped[datetime.date] = mapped_column()
    generated: Mapped[str] = mapped_column()
    efficiency: Mapped[str] = mapped_column()
    exported: Mapped[str] = mapped_column()
    peak_power: Mapped[str] = mapped_column()
    peak_time: Mapped[datetime.time] = mapped_column(nullable=True)
    conditions: Mapped[str] = mapped_column()


class Weekly(Base):
    __tablename__ = "weekly"

    pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sid: Mapped[int] = mapped_column(ForeignKey("system.sid"))
    system: Mapped[Optional["System"]] = relationship(back_populates="weekly")
    year: Mapped[int] = mapped_column()
    week: Mapped[int] = mapped_column()
    generated: Mapped[str] = mapped_column()
    efficiency: Mapped[str] = mapped_column()
    exported: Mapped[str] = mapped_column()
    fit_credit: Mapped[str] = mapped_column()
    low: Mapped[str] = mapped_column()
    high: Mapped[str] = mapped_column()
    average: Mapped[str] = mapped_column()


class Monthly(Base):
    __tablename__ = "monthly"

    pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sid: Mapped[int] = mapped_column(ForeignKey("system.sid"))
    system: Mapped[Optional["System"]] = relationship(back_populates="monthly")
    year: Mapped[int] = mapped_column()
    month: Mapped[int] = mapped_column()
    generated: Mapped[str] = mapped_column()
    efficiency: Mapped[str] = mapped_column()
    exported: Mapped[str] = mapped_column()
    fit_credit: Mapped[str] = mapped_column()
    low: Mapped[str] = mapped_column()
    high: Mapped[str] = mapped_column()
    average: Mapped[str] = mapped_column()


class Yearly(Base):
    __tablename__ = "yearly"

    pk: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sid: Mapped[int] = mapped_column(ForeignKey("system.sid"))
    system: Mapped[Optional["System"]] = relationship(back_populates="yearly")
    year: Mapped[int] = mapped_column()
    generated: Mapped[str] = mapped_column()
    efficiency: Mapped[str] = mapped_column()
    exported: Mapped[str] = mapped_column()
    fit_credit: Mapped[str] = mapped_column()
    low: Mapped[str] = mapped_column()
    high: Mapped[str] = mapped_column()
    average: Mapped[str] = mapped_column()