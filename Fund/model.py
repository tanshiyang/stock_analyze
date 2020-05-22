# coding: utf-8
from sqlalchemy import Column, Float, Index, String, Text
from sqlalchemy.dialects.mysql import TEXT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class FundBasic(Base):
    __tablename__ = 'fund_basic'

    ts_code = Column(TEXT, primary_key=True)
    name = Column(TEXT, nullable=False)
    management = Column(Text)
    custodian = Column(Text)
    fund_type = Column(Text)
    found_date = Column(Text)
    due_date = Column(Text)
    list_date = Column(Text)
    issue_date = Column(Text)
    delist_date = Column(Text)
    issue_amount = Column(Float(asdecimal=True))
    m_fee = Column(Float(asdecimal=True))
    c_fee = Column(Float(asdecimal=True))
    duration_year = Column(Float(asdecimal=True))
    p_value = Column(Float(asdecimal=True))
    min_amount = Column(Float(asdecimal=True))
    exp_return = Column(Text)
    benchmark = Column(Text)
    status = Column(Text)
    invest_type = Column(Text)
    type = Column(Text)
    trustee = Column(Text)
    purc_startdate = Column(Text)
    redm_startdate = Column(Text)
    market = Column(Text)


class FundNav(Base):
    __tablename__ = 'fund_nav'
    __table_args__ = (
        Index('ts_code', 'ts_code', 'end_date', unique=True),
    )

    ts_code = Column(VARCHAR(20), primary_key=True, nullable=False)
    ann_date = Column(VARCHAR(20), nullable=False)
    end_date = Column(VARCHAR(20), primary_key=True, nullable=False)
    unit_nav = Column(Float)
    accum_nav = Column(Float)
    accum_div = Column(Float)
    net_asset = Column(Float)
    total_netasset = Column(Float)


class FundPortfolio(Base):
    __tablename__ = 'fund_portfolio'
    __table_args__ = (
        Index('ts_code', 'ts_code', 'end_date','symbol', unique=True),
    )

    ts_code = Column(VARCHAR(20), primary_key=True, nullable=False)
    ann_date = Column(String(20))
    end_date = Column(VARCHAR(20), primary_key=True, nullable=False)
    symbol = Column(String(500), primary_key=True, nullable=False)
    mkv = Column(Float)
    amount = Column(Float)
    stk_mkv_ratio = Column(Float)
    stk_float_ratio = Column(Float)
