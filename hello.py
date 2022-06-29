from datetime import datetime

from vnpy.trader.constant import Exchange
from vnpy.trader.object import HistoryRequest

from vnpy_akshare.akshre_feed import AKShareDataFeed

# req = HistoryRequest('000001', Exchange.SZSE, datetime.strptime("2018-01-01", "%Y-%m-%d"), datetime.strptime("2018-02-01", "%Y-%m-%d"))
# print(AKShareDataFeed().query_bar_history(req))

# req = HistoryRequest('600276', Exchange.SSE, datetime.strptime("2018-01-01", "%Y-%m-%d"), datetime.strptime("2018-02-01", "%Y-%m-%d"))
# print(AKShareDataFeed().query_bar_history(req))


req = HistoryRequest('600276', Exchange.SHFE, datetime.strptime("2018-01-01", "%Y-%m-%d"), datetime.strptime("2018-02-01", "%Y-%m-%d"))
print(AKShareDataFeed().query_bar_history(req))