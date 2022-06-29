import dataclasses
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd
from pytz import timezone

from numpy import ndarray
from pandas import DataFrame

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import round_to
from vnpy.trader.datafeed import BaseDatafeed

import akshare as ak

INTERVAL_VT2RQ: Dict[Interval, str] = {
    Interval.DAILY: "daily",
    Interval.WEEKLY: "weekly",
}

INTERVAL_ADJUSTMENT_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(hours=-15)         # no need to adjust for daily bar
}

CHINA_TZ = timezone("Asia/Shanghai")


def string_to_date(ds: str) -> datetime:
    return datetime.strptime(ds, "%Y-%m-%d")


def date_to_string(dd: datetime) -> str:
    if dd is None:
        return None
    return dd.strftime("%Y%m%d")


@dataclasses.dataclass
class TradeDate:
    start:datetime
    end: datetime
    date_list: list[datetime]


class Country(Enum):
    China = "china"
    US = "us"
    UK = "uk"


country_trade_date: Dict[Country, TradeDate or None] = {
    Country.China: None,
    Country.US: None,
    Country.UK: None,
}


EXCHANGE_COUNTRY = {
    Country.China: {
        Exchange.CFFEX,
        Exchange.SHFE,
        Exchange.CZCE,
        Exchange.DCE,
        Exchange.INE,
        Exchange.SSE,
        Exchange.SZSE,
        Exchange.BSE,
        Exchange.SGE,
        Exchange.WXE,
        Exchange.CFETS,
        Exchange.XBOND,
    },
}


def get_country(exchange: Exchange):
    for country, exchange_set in EXCHANGE_COUNTRY.items():
        if exchange in exchange_set:
            return country

    return None


def get_zh_a_trader_date():
    date_list = list(ak.stock_zh_index_daily_tx("sh000919").date)
    date_list = [string_to_date(d) for d in date_list]
    start = date_list[0]
    end = date_list[-1]
    return TradeDate(start, end, date_list)


def get_trade_date(exchange, start: datetime, end: datetime)-> List[datetime]:
    country = get_country(exchange)
    td = country_trade_date[country]
    if td is None:
        if country == Country.China:
            td = get_zh_a_trader_date()
        country_trade_date[country] = td

    return [d for d in td.date_list if end >= d >= start]


class BaseFeed:
    def query_bar_history(self, req: HistoryRequest) -> pd.DataFrame:

        pass

    def query_tick_history(self, req: HistoryRequest) -> pd.DataFrame:
        pass


class ZhADataFeed(BaseFeed):
    def query_bar_history(self, req: HistoryRequest) -> pd.DataFrame:
        symbol: str = req.symbol
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        if interval is None:
            interval = Interval.DAILY

        period = INTERVAL_VT2RQ[interval]
        df = ak.stock_zh_a_hist(symbol, period, date_to_string(start), date_to_string(end), "hfq")

        df.rename(columns={
            '日期': "datetime",
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'turnover',
        }, inplace=True)
        return df

    def query_tick_history(self, req: HistoryRequest) -> pd.DataFrame:
        symbol: str = req.symbol
        start: datetime = req.start
        end: datetime = req.end

        if end is None:
            end = datetime.now()

        date_list = get_trade_date(req.exchange, start, end)
        ret = []
        for d in date_list:
            ret.append(ak.stock_zh_a_tick_163(symbol, date_to_string(d)))

        return pd.concat(ret)


class ZhFutureDataFeed(BaseFeed):
    def query_bar_history(self, req: HistoryRequest) -> pd.DataFrame:
        symbol: str = req.symbol

        start: datetime = req.start
        end: datetime = req.end
        exchange = req.exchange

        df = ak.get_futures_daily(date_to_string(start), date_to_string(end), exchange.value)

        return df

    def query_tick_history(self, req: HistoryRequest) -> pd.DataFrame:
        symbol: str = req.symbol
        start: datetime = req.start
        end: datetime = req.end

        if end is None:
            end = datetime.now()

        date_list = get_trade_date(req.exchange, start, end)
        ret = []
        for d in date_list:
            ret.append(ak.stock_zh_a_tick_163(symbol, date_to_string(d)))

        return pd.concat(ret)


FEEDS = {
    Exchange.CFFEX: ZhFutureDataFeed,
    Exchange.SHFE: ZhFutureDataFeed,
    Exchange.CZCE: ZhFutureDataFeed,
    Exchange.DCE: ZhFutureDataFeed,
    Exchange.INE: ZhFutureDataFeed,

    Exchange.SSE: ZhADataFeed,
    Exchange.SZSE: ZhADataFeed,
    Exchange.BSE: ZhADataFeed,
}


class AKShareDataFeed(BaseDatafeed):
    """AKData数据服务接口"""

    def __init__(self):
        self.inited = False

    def init(self) -> bool:
        self.inited = True
        return True

    def convert_df_to_bar(self, req: HistoryRequest, df: DataFrame) -> Optional[List[BarData]]:

        data: List[BarData] = []

        interval: Interval = req.interval if req.interval is not None else Interval.DAILY

        # 为了将时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                dt: datetime = string_to_date(row.datetime)
                dt: datetime = dt - adjustment
                dt: datetime = CHINA_TZ.localize(dt)

                bar: BarData = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="AK"
                )

                data.append(bar)

        return data

    def convert_df_to_tick(self, df: DataFrame) -> Optional[List[TickData]]:
        return df

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init()
            if not n:
                return []

        exchange: Exchange = req.exchange
        if exchange not in FEEDS:
            return []

        clazz = FEEDS[exchange]
        df = clazz().query_bar_history(req)

        return self.convert_df_to_bar(req, df)

    def query_tick_history(self, req: HistoryRequest) -> Optional[List[TickData]]:
        exchange: Exchange = req.exchange
        if exchange not in FEEDS:
            return []

        clazz = FEEDS[exchange]

        df = clazz().query_tick_history(req)
        return self.convert_df_to_tick(df)


# def to_rq_symbol(symbol: str, exchange: Exchange) -> str:
#     """将交易所代码转换为米筐代码"""
#     # 股票
#     if exchange in [Exchange.SSE, Exchange.SZSE]:
#         if exchange == Exchange.SSE:
#             rq_symbol: str = f"{symbol}.XSHG"
#         else:
#             rq_symbol: str = f"{symbol}.XSHE"
#     # 金交所现货
#     elif exchange in [Exchange.SGE]:
#         for char in ["(", ")", "+"]:
#             symbol: str = symbol.replace(char, "")
#         symbol = symbol.upper()
#         rq_symbol: str = f"{symbol}.SGEX"
#     # 期货和期权
#     elif exchange in [Exchange.SHFE, Exchange.CFFEX, Exchange.DCE, Exchange.CZCE, Exchange.INE]:
#         for count, word in enumerate(symbol):
#             if word.isdigit():
#                 break
#
#         product: str = symbol[:count]
#         time_str: str = symbol[count:]
#
#         # 期货
#         if time_str.isdigit():
#             if exchange is not Exchange.CZCE:
#                 return symbol.upper()
#
#             # 检查是否为连续合约或者指数合约
#             if time_str in ["88", "888", "99", "889"]:
#                 return symbol
#
#             year: str = symbol[count]
#             month: str = symbol[count + 1:]
#
#             if year == "9":
#                 year = "1" + year
#             else:
#                 year = "2" + year
#
#             rq_symbol: str = f"{product}{year}{month}".upper()
#         # 期权
#         else:
#             if exchange in [Exchange.CFFEX, Exchange.DCE, Exchange.SHFE]:
#                 rq_symbol: str = symbol.replace("-", "").upper()
#             elif exchange == Exchange.CZCE:
#                 year: str = symbol[count]
#                 suffix: str = symbol[count + 1:]
#
#                 if year == "9":
#                     year = "1" + year
#                 else:
#                     year = "2" + year
#
#                 rq_symbol: str = f"{product}{year}{suffix}".upper()
#     else:
#         rq_symbol: str = f"{symbol}.{exchange.value}"
#
#     return rq_symbol
#
#
# class RqdataDatafeed(BaseDatafeed):
#     """米筐RQData数据服务接口"""
#
#     def __init__(self):
#         """"""
#         self.username: str = SETTINGS["datafeed.username"]
#         self.password: str = SETTINGS["datafeed.password"]
#
#         self.inited: bool = False
#         self.symbols: ndarray = None
#
#     def init(self) -> bool:
#         """初始化"""
#         if self.inited:
#             return True
#
#         if not self.username or not self.password:
#             return False
#
#         try:
#             init(
#                 self.username,
#                 self.password,
#                 ("rqdatad-pro.ricequant.com", 16011),
#                 use_pool=True,
#                 max_pool_size=1,
#                 auto_load_plugins=False
#             )
#
#             df: DataFrame = all_instruments()
#             self.symbols = df["order_book_id"].values
#         except (RuntimeError, AuthenticationFailed):
#             return False
#
#         self.inited = True
#         return True
#
#     def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
#         """查询K线数据"""
#         if not self.inited:
#             n: bool = self.init()
#             if not n:
#                 return []
#
#         symbol: str = req.symbol
#         exchange: Exchange = req.exchange
#         interval: Interval = req.interval
#         start: datetime = req.start
#         end: datetime = req.end
#
#         rq_symbol: str = to_rq_symbol(symbol, exchange)
#
#         rq_interval: str = INTERVAL_VT2RQ.get(interval)
#         if not rq_interval:
#             return None
#
#         # 为了将米筐时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
#         adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]
#
#         # 为了查询夜盘数据
#         end += timedelta(1)
#
#         # 只对衍生品合约才查询持仓量数据
#         fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]
#         if not symbol.isdigit():
#             fields.append("open_interest")
#
#         df: DataFrame = get_price(
#             rq_symbol,
#             frequency=rq_interval,
#             fields=fields,
#             start_date=start,
#             end_date=end,
#             adjust_type="none"
#         )
#
#         data: List[BarData] = []
#
#         if df is not None:
#             # 填充NaN为0
#             df.fillna(0, inplace=True)
#
#             for row in df.itertuples():
#                 dt: datetime = row.Index[1].to_pydatetime() - adjustment
#                 dt: datetime = CHINA_TZ.localize(dt)
#
#                 bar: BarData = BarData(
#                     symbol=symbol,
#                     exchange=exchange,
#                     interval=interval,
#                     datetime=dt,
#                     open_price=round_to(row.open, 0.000001),
#                     high_price=round_to(row.high, 0.000001),
#                     low_price=round_to(row.low, 0.000001),
#                     close_price=round_to(row.close, 0.000001),
#                     volume=row.volume,
#                     turnover=row.total_turnover,
#                     open_interest=getattr(row, "open_interest", 0),
#                     gateway_name="RQ"
#                 )
#
#                 data.append(bar)
#
#         return data
#
#     def query_tick_history(self, req: HistoryRequest) -> Optional[List[TickData]]:
#         """查询Tick数据"""
#         if not self.inited:
#             n: bool = self.init()
#             if not n:
#                 return []
#
#         symbol: str = req.symbol
#         exchange: Exchange = req.exchange
#         start: datetime = req.start
#         end: datetime = req.end
#
#         rq_symbol: str = to_rq_symbol(symbol, exchange)
#         if rq_symbol not in self.symbols:
#             return None
#
#         # 为了查询夜盘数据
#         end += timedelta(1)
#
#         # 只对衍生品合约才查询持仓量数据
#         fields: list = [
#             "open",
#             "high",
#             "low",
#             "last",
#             "prev_close",
#             "volume",
#             "total_turnover",
#             "limit_up",
#             "limit_down",
#             "b1",
#             "b2",
#             "b3",
#             "b4",
#             "b5",
#             "a1",
#             "a2",
#             "a3",
#             "a4",
#             "a5",
#             "b1_v",
#             "b2_v",
#             "b3_v",
#             "b4_v",
#             "b5_v",
#             "a1_v",
#             "a2_v",
#             "a3_v",
#             "a4_v",
#             "a5_v",
#         ]
#         if not symbol.isdigit():
#             fields.append("open_interest")
#
#         df: DataFrame = get_price(
#             rq_symbol,
#             frequency="tick",
#             fields=fields,
#             start_date=start,
#             end_date=end,
#             adjust_type="none"
#         )
#
#         data: List[TickData] = []
#
#         if df is not None:
#             # 填充NaN为0
#             df.fillna(0, inplace=True)
#
#             for row in df.itertuples():
#                 dt: datetime = row.Index[1].to_pydatetime()
#                 dt: datetime = CHINA_TZ.localize(dt)
#
#                 tick: TickData = TickData(
#                     symbol=symbol,
#                     exchange=exchange,
#                     datetime=dt,
#                     open_price=row.open,
#                     high_price=row.high,
#                     low_price=row.low,
#                     pre_close=row.prev_close,
#                     last_price=row.last,
#                     volume=row.volume,
#                     turnover=row.total_turnover,
#                     open_interest=getattr(row, "open_interest", 0),
#                     limit_up=row.limit_up,
#                     limit_down=row.limit_down,
#                     bid_price_1=row.b1,
#                     bid_price_2=row.b2,
#                     bid_price_3=row.b3,
#                     bid_price_4=row.b4,
#                     bid_price_5=row.b5,
#                     ask_price_1=row.a1,
#                     ask_price_2=row.a2,
#                     ask_price_3=row.a3,
#                     ask_price_4=row.a4,
#                     ask_price_5=row.a5,
#                     bid_volume_1=row.b1_v,
#                     bid_volume_2=row.b2_v,
#                     bid_volume_3=row.b3_v,
#                     bid_volume_4=row.b4_v,
#                     bid_volume_5=row.b5_v,
#                     ask_volume_1=row.a1_v,
#                     ask_volume_2=row.a2_v,
#                     ask_volume_3=row.a3_v,
#                     ask_volume_4=row.a4_v,
#                     ask_volume_5=row.a5_v,
#                     gateway_name="RQ"
#                 )
#
#                 data.append(tick)
#
#         return data