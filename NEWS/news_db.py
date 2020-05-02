from NEWS.model import News, Newskeyword, StockBasic
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pandas import DataFrame, read_sql
import uuid
import datetime

conn_string = 'mysql://root:123123@localhost/fns?charset=utf8'


def save_news_to_db(result_df=DataFrame):
    engine = create_engine(conn_string)
    conn = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    for news_index, news_row in result_df.iterrows():
        news = News()
        news.ID = uuid.uuid1()
        news.NewsTime = news_row["date_time"]
        news.KeywordRule = ','.join(news_row["keywords"])
        news.KeywordGroup = ','.join(news_row["keywords_group"])
        news.Content = news_row["content"]
        news.Title = news_row["content"]
        news.Src = news_row["src"]
        news.CreateBy = "system"
        news.CreateTime = datetime.datetime.utcnow()
        session.add(news)

    session.commit()
    conn.close()
    engine.dispose()


def save_stock_basic_to_db(result_df=DataFrame):
    engine = create_engine(conn_string)
    conn = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()
    session.query(StockBasic).delete()
    for news_index, row in result_df.iterrows():
        stock = StockBasic()
        stock.ID = row["ts_code"]  # uuid.uuid1()
        stock.ts_code = row["ts_code"]
        stock.symbol = row["symbol"]
        stock.name = row["name"]
        stock.area = row["area"]
        stock.industry = row["industry"]
        stock.list_date = row["list_date"]
        stock.CreateBy = "system"
        stock.CreateTime = datetime.datetime.utcnow()
        session.add(stock)

    session.commit()
    conn.close()
    engine.dispose()

def get_news_keywords():
    engine = create_engine(conn_string)
    conn = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    result = session.query(Newskeyword).all()

    conn.close()
    engine.dispose()
    return result
