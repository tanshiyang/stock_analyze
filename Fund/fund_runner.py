import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from Fund import fund_nav,fund_portfolio,fund_basic,calc_volatility

if __name__ == '__main__':
    fund_basic.collect_fund_basic()
    # fund_nav.collect_fund_nav() #积分不够2000
    fund_portfolio.collect_fund_portfolio()
    calc_volatility.calc_year_data()
