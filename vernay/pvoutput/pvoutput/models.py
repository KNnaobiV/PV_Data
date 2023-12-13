import configparser
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
    "Location",
    "Monthly",
    "System",
    "SystemInfo",
    "Weekly",
    "Yearly"
]

Base = declarative_base()

def create_tables(engine):
    Base.metadata.create_all(engine, checkfirst=True)


class Country(Base):
    __tablename__ = "country"

    sid: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    systems: Mapped[Optional["System"]] = relationship(back_populates="country", uselist=True)


class System(Base): 
    __tablename__ = "system"

    sid: Mapped[int] = mapped_column(primary_key=True)
    id: Mapped[int] = mapped_column()
    info: Mapped[Optional["SystemInfo"]] = relationship(uselist=False, back_populates='system')
    name: Mapped[str] = mapped_column()
    daily: Mapped[Optional["Daily"]] = relationship(uselist=False, back_populates='system')
    weekly: Mapped[Optional["Weekly"]] = relationship(uselist=False, back_populates='system')
    monthly: Mapped[Optional["Monthly"]] = relationship(uselist=False, back_populates='system')
    yearly: Mapped[Optional["Yearly"]] = relationship(uselist=False, back_populates='system')
    country_sid: Mapped[Optional[int]] = mapped_column(ForeignKey("country.sid"))
    country: Mapped[Optional["Country"]] = relationship(back_populates="systems")
    location: Mapped[Optional["Location"]] = relationship(back_populates='system')


class Location(Base):
    __tablename__ = "location"

    # id: Mapped[int] = mapped_column(primary_key=True)
    latitude: Mapped[float] = mapped_column()
    longitude: Mapped[float] = mapped_column()
    system_sid: Mapped[int] = mapped_column(ForeignKey("system.sid"), primary_key=True)
    system: Mapped[Optional["System"]] = relationship(back_populates="location")


class SystemInfo(Base):
    __tablename__ = "info"

    system_sid: Mapped[Optional[int]] = mapped_column(ForeignKey("system.sid"), primary_key=True)
    system: Mapped[Optional["System"]] = relationship(back_populates="info")
    name: Mapped[str] = mapped_column()
    number_of_panels: Mapped[int] = mapped_column()
    panel_max_power: Mapped[float] = mapped_column()
    size: Mapped[float] = mapped_column()
    panel_brand: Mapped[str] = mapped_column()
    orientation: Mapped[str] = mapped_column()
    number_of_inverters: Mapped[int] = mapped_column()
    inverter_brand: Mapped[str] = mapped_column()
    inverter_size: Mapped[float] = mapped_column()
    post_code: Mapped[str] = mapped_column()
    installation_date: Mapped[str] = mapped_column()
    shading: Mapped[str] = mapped_column()
    tilt: Mapped[str] = mapped_column()
    comments: Mapped[str] = mapped_column()


class Daily(Base):
    __tablename__ = "daily"

    system_sid: Mapped[int] = mapped_column(ForeignKey("system.sid"), primary_key=True)
    system: Mapped[Optional["System"]] = relationship(back_populates="daily")
    date = None
    generated: Mapped[int] = mapped_column()
    efficiency: Mapped[int] = mapped_column()
    exported: Mapped[int] = mapped_column()
    peak_power: Mapped[int] = mapped_column()
    peak_time: Mapped[int] = mapped_column()
    conditions: Mapped[str] = mapped_column()


class Weekly(Base):
    __tablename__ = "weekly"

    system_sid: Mapped[int] = mapped_column(ForeignKey("system.sid"), primary_key=True)
    system: Mapped[Optional["System"]] = relationship(back_populates="weekly")
    period: Mapped[str] = mapped_column()
    generated: Mapped[int] = mapped_column()
    efficiency: Mapped[int] = mapped_column()
    exported: Mapped[int] = mapped_column()
    fit_credit: Mapped[str] = mapped_column()
    low: Mapped[int] = mapped_column()
    high: Mapped[int] = mapped_column()
    average: Mapped[int] = mapped_column()


class Monthly(Base):
    __tablename__ = "monthly"

    system_sid: Mapped[int] = mapped_column(ForeignKey("system.sid"), primary_key=True)
    system: Mapped[Optional["System"]] = relationship(back_populates="monthly")
    month: Mapped[str] = mapped_column()
    generated: Mapped[int] = mapped_column()
    efficiency: Mapped[int] = mapped_column()
    exported: Mapped[int] = mapped_column()
    fit_credit: Mapped[str] = mapped_column()
    low: Mapped[int] = mapped_column()
    high: Mapped[int] = mapped_column()
    average: Mapped[int] = mapped_column()


class Yearly(Base):
    __tablename__ = "yearly"

    system_sid: Mapped[int] = mapped_column(ForeignKey("system.sid"), primary_key=True)
    system: Mapped[Optional["System"]] = relationship(back_populates="yearly")
    year: Mapped[str] = mapped_column()
    generated: Mapped[int] = mapped_column()
    efficiency: Mapped[int] = mapped_column()
    exported: Mapped[int] = mapped_column()
    fit_credit: Mapped[str] = mapped_column()
    low: Mapped[int] = mapped_column()
    high: Mapped[int] = mapped_column()
    average: Mapped[int] = mapped_column()