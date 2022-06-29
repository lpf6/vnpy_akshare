import datetime as dt
from collections import Iterable
from enum import Enum, unique
from functools import lru_cache

import numpy as np
import pandas as pd

import vnpy_akshare.utils.date_utils as du
from vnpy_akshare.utils.log import log
from vnpy_akshare.utils.thread_util import parallelize_dataframe


@unique
class Type(Enum):
    STOCK = "s"
    INDEX = "i"
    FUND = "f"
    ETF = "e"
    LOF = "L"
    BOND = "b"
    TBOND = "t"
    FUTURES = "fu"
    OPTIONS = "o"
    UNKNOWN = "u"


class Wrap(object):
    def get_buy_code(self, code):
        return code

    def get_type(self, type=[]):
        '''类型转换'''
        pass

    def get_all_securities(self, dtype=[], date=None) -> pd.DataFrame:
        '''
        获取所有标的
        :param type:
        :param date:
        :return: display_name: str, code: str, start_date: str, end_date: str
        '''
        pass

    def get_cache(self, key):
        pass

    def put_cache(self, key, value):
        pass

    @lru_cache(maxsize=2 ** 32)
    def get_cached_data(self, start_date, end_date, gen_key, filter_stocks,
                        get_and_process_data, cached, cache_end, update_all, split_year=True):

        if split_year:
            years = [dt.date(year=y, month=1, day=1) for y in range(start_date.year + 1, end_date.year)]
        else:
            years = []
        years.insert(0, start_date)
        years.append(end_date)

        all_data = []
        lack_dates = []
        for i in range(len(years) - 1):
            start = years[i]
            end = years[i + 1] if i == len(years) - 2 else years[i + 1] - dt.timedelta(1)

            start_d = None
            end_d = None
            if update_all:
                start_d = start
                end_d = end
            else:
                for day in du.trade_range(start, end):
                    key = gen_key(day)
                    d = self.get_cache(key) if cached and (day != end_date or cache_end) else None
                    if d is None or len(d) == 0:
                        if start_d is None:
                            start_d = day
                        end_d = day
                    elif end_d is not None:
                        lack_dates.append([start_d, end_d])
                        start_d = end_d = None
                    if d is not None:
                        if len(d) > 0:
                            all_data.append(d)
            if end_d is not None:
                lack_dates.append([start_d, end_d])

        log.info("get_cached_daily_data: lack_dates: %s" % lack_dates)
        for date in lack_dates:
            iter_securities = filter_stocks(date[0], date[1], False)
            if len(iter_securities) > 0:
                single_data = get_and_process_data(iter_securities, date[0], date[1])
                if single_data is not None and len(single_data) > 0:
                    all_data.append(single_data)

        if len(all_data) == 0:
            return None
        else:
            all_data = pd.concat(all_data)
        all_data = all_data.drop_duplicates(["code", "date"], keep='first')
        all_data = all_data.sort_values(["code", "date"], ascending=True).reset_index(drop=True)

        if cached:
            all_lack_dates = []
            for date in lack_dates:
                all_lack_dates.extend([i for i in du.trade_range(date[0], date[1])])
            if len(all_lack_dates) > 0:
                all_dates = all_data["date"].unique()
                func = None
                if type(all_dates[0]) == str or type(all_dates[0]) == np.str:
                    func = du.to_str
                if type(all_lack_dates[0]) == dt.datetime or type(all_dates[0]) == np.datetime64:
                    func = np.datetime64
                if func is not None:
                    all_lack_dates = [func(d) for d in all_lack_dates]
                all_lack_dates = np.asarray(all_lack_dates)
                data_save = all_data[all_data["date"].isin(all_lack_dates)]
                for d, group in data_save.groupby(by="date"):
                    key = gen_key(d)
                    self.put_cache(key, group)
        return all_data

    def get_cached_daily_data(self, start_date, end_date, gen_key, filter_stocks,
                              get_and_process_data, cached, cache_end, update_all, split_year=True, **kwargs):
        start_date = du.to_date(start_date)
        end_date = du.to_date(end_date)

        all_data = self.get_cached_data(start_date, end_date, gen_key, filter_stocks,
                                        get_and_process_data, cached, cache_end, update_all, split_year)
        self.process_data(**kwargs)
        # all_data = all_data[(all_data["date"] >= start_date) & (all_data["date"] <= end_date)]
        # all_data["code"] = all_data["code"].apply(lambda c: self.make_symbol(c))
        log.info("get_cached_daily_data: all_data: %s" % len(all_data))
        return all_data.reset_index(drop=True)

    def process_data(self, all_data: pd.DataFrame, **kwargs):
        filter_stocks = kwargs.pop("filter_stocks", None)
        if filter_stocks:
            security = filter_stocks()

        all_data = all_data[all_data["code"].isin(set(security))]
        # all_data["code"] = parallelize_dataframe(
        #     all_data["code"], lambda d: d.apply(self.make_symbol))
        return all_data

    def get_price(self, security, start_date=None, end_date=None, frequency='daily', dtype=None,
                  fields=None, fq='pre', count=None, cached=True, cache_end=False,
                  update_all=False, filter=True) -> pd.DataFrame:
        '''
        获取价格
        :param security:
        :param start_date:
        :param end_date:
        :param frequency:
        :param fields:
        :param fq:
        :param count:
        :param cached:
        :param cache_end:
        :return:
        '''
        pass

    def get_fund_price(self, security, start_date=None, end_date=None, frequency='daily',
                       fields=None, fq='pre', count=None, cached=True, cache_end=False,
                       update_all=False, filter=True) -> pd.DataFrame:
        '''
        获取价格
        :param security:
        :param start_date:
        :param end_date:
        :param frequency:
        :param fields:
        :param fq:
        :param count:
        :param cached:
        :param cache_end:
        :return:
        '''
        pass

    def get_fund_data(self, security: str or list, start_date=None, end_date=None
                      , count=None, cached=False, cache_end=False, update_all=False, filter=True):
        pass

    def make_symbol(self, security):
        '''
        返回标准symbol字符串
        :param security:
        :return:
        '''
        pass

    def get_symbol(self, security):
        '''
        返回目标平台形式的字符串
        :param security:
        :return:
        '''
        pass

    def get_symbol_info(self, security: str or list, get_type_func=None) -> tuple or list:
        '''
        获取标的信息，数组形式
        :param security:
        :param get_type_func:
        :return: 数组形式
        '''
        is_single = False
        if type(security) is str or not isinstance(security, Iterable):
            is_single = True
            security = [security]

        ret = []
        for sec in security:
            if type(sec) is str or type(sec) is np.str:
                code = sec[:6] if '0' <= sec[0] <= '9' else sec[-6:]
                d_type = Type.STOCK
                if len(sec) == 6:
                    pass
                elif code[:2] in {"15", "16", "50", "51", "52"}:
                    d_type = Type.ETF
                elif get_type_func is None:
                    for n, e in Type.__members__.items():
                        if sec.endswith(e.value):
                            d_type = e
                            break
                else:
                    if type(sec) is str or type(sec) is np.str:
                        for n, e in Type.__members__.items():
                            if sec.endswith(e.value):
                                d_type = e
                                sec = (sec[:6] if '0' <= sec[0] <= '9' else sec[-6:], d_type)
                                break
                    code, d_type = get_type_func(sec)
            else:
                code, d_type = get_type_func(sec)
            ret.append((code, d_type))

        if is_single:
            return ret[0]
        return ret

    def _make_symbol(self, security: str or list, d_type=None):
        is_single = False
        if type(security) is str or not isinstance(security, Iterable):
            is_single = True
            security = [security, d_type if d_type is not None else Type.STOCK]
        elif type(security) is tuple and len(security) == 2 and type(security[1]) == Type:
            is_single = True
            security = [security]

        ret = []
        for sec in security:
            s = sec[0]
            t = sec[1]
            ret.append(s + "." + t.value)

        if is_single:
            return ret[0]
        return ret

    def _get_symbol(self, security, type2sec_dic, get_type_func=None):
        info = self.get_symbol_info(security, get_type_func)

        is_single = False
        if type(security) is str or not isinstance(info, Iterable):
            is_single = True
            info = [info]

        ret = []
        for i in info:
            sec = i[0]
            sec_deal = type2sec_dic[i[1]]
            if sec_deal is None:
                pass
            elif hasattr(sec_deal, '__call__'):
                sec = sec_deal(sec, i[1])
            elif type(sec_deal) is str:
                sec = sec + sec_deal
            else:
                raise TypeError("Unknown type %s for deal security %s" % (type(sec_deal), sec_deal))

            ret.append(sec)

        if is_single:
            return ret[0]
        return ret

    def _get_type(self, types: list, type_dic: dict):

        is_single = False
        if not isinstance(types, Iterable):
            is_single = True
            types = [types]

        ret = []
        for t in types:
            type_str = type_dic[t]
            ret.append(type_str)

        if is_single:
            return ret[0]
        return ret

    def _rev_type(self, types: list, type_dic: dict = None, rev_type: dict = None):

        is_single = False
        if not isinstance(types, Iterable):
            is_single = True
            types = [types]

        if rev_type is None:
            dic = {}
            for k, v in type_dic.items():
                if v is not None:
                    dic[v] = k
        else:
            dic = rev_type
        ret = []
        for t in types:
            type_str = dic[t]
            ret.append(type_str)

        if is_single:
            return ret[0]
        return ret

    def get_day_data(self, start_date, end_date, saved=True):
        pass

    def get_index_weight(self, index, date):
        pass
