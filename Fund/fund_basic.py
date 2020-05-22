from Stock import mydb
import mytusharepro
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey
pro = mytusharepro.MyTusharePro()


def collect_fund_basic_work():
    fund_basic = pro.fund_basic(market='O', fields='ts_code,name,management,custodian,fund_type,'
                                                    'found_date,due_date,list_date,issue_date,delist_date,'
                                                    'issue_amount,m_fee,c_fee,duration_year,p_value,'
                                                    'min_amount,exp_return,benchmark,status,invest_type,'
                                                    'type,trustee,purc_startdate,redm_startdate,market')
    engine = mydb.engine()
    fund_basic.to_sql('fund_basic', engine, index=False, if_exists='replace')


def collect_fund_basic():
    engine = mydb.engine()
    conn = mydb.conn()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table('fund_basic', metadata,
                  Column('ts_code', String(20), primary_key=True),
                  Column('name', String(500)),
                  Column('management', String(500)),
                  Column('custodian', String(500)),
                  Column('fund_type', String(500)),
                  Column('found_date', String(500)),
                  Column('due_date', String(500)),
                  Column('list_date', String(500)),
                  Column('issue_date', String(500)),
                  Column('delist_date', String(500)),
                  Column('issue_amount', Float),
                  Column('m_fee', Float),
                  Column('c_fee', Float),
                  Column('duration_year', Float),
                  Column('p_value', Float),
                  Column('min_amount', Float),
                  Column('exp_return', Float),
                  Column('benchmark', String(500)),
                  Column('status', String(500)),
                  Column('invest_type', String(500)),
                  Column('type', String(500)),
                  Column('trustee', String(500)),
                  Column('purc_startdate', String(500)),
                  Column('redm_startdate', String(500)),
                  Column('market', String(500)),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)

    collect_fund_basic_work()
    conn.close()


if __name__ == '__main__':
    collect_fund_basic()
