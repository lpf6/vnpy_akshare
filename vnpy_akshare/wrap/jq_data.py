import math
import os
import pathlib
from collections import Iterable
from functools import lru_cache

import akshare as ak
import numpy as np
import pandas as pd
from diskcache import Cache

import vnpy_akshare.utils.date_utils as du
from vnpy_akshare.utils.execpt import except_method
from vnpy_akshare.utils.log import cache_path as get_cache_path, info_path as get_info_path
from .wrap import Wrap, Type


class Wrapper(Wrap):
    cache_path = get_cache_path("cache")
    info_path = get_info_path("jq.json")

    _parent = os.path.dirname(os.path.dirname(__file__))
    _cache = Cache(cache_path)
    _cache_expire = 80 * 365 * 24 * 60 * 60

    def __new__(cls, *args, **kwargs):
        if '_instance' not in vars(cls):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._all_stocks = None
        self._all_securities = None
        self._filter_securities = None

    def get_symbol(self, security):
        return self._get_symbol(security, Wrapper.type_name_dict)

    @staticmethod
    @except_method(try_count=5)
    def _get_data(func) -> pd.DataFrame:
        return func()

    @lru_cache()
    def _get_gen_price_key(self, frequency, fq, prefix=None):
        def gen_key(day):
            if prefix:
                return prefix + "_" + frequency + "_" + du.to_str(day) + "_" + fq
            return frequency + "_" + du.to_str(day) + "_" + fq

        return gen_key

    @lru_cache()
    def _get_gen_fund_key(self):
        def gen_key(day):
            return "fund_" + du.to_str(day)

        return gen_key

    def filter_stocks(self, start_date, end_date, need):
        all_stocks = self._all_stocks
        all_securities = self._all_securities
        request_securities = self._filter_securities
        if need:
            return request_securities
        iter_securities = all_securities
        current_stocks = list(all_stocks.loc[(all_stocks["start_date"] <= end_date) & (
                all_stocks["end_date"] >= start_date)].index)
        iter_securities = list(set(iter_securities) & set(current_stocks))
        return iter_securities

    @lru_cache()
    def _get_and_process_price_data(self, frequency, fields, fq):
        def get_and_process_data(securities, start_date, end_date):
            d = Wrapper._get_data(
                lambda: jq.get_price(
                    securities, start_date=start_date, end_date=end_date,
                    frequency=frequency, fields=fields, skip_paused=True, fq=fq, panel=False))
            d = d.dropna()
            if "time" in d.columns:
                d.rename(columns={"time": "date"}, inplace=True)
            elif "date" not in d.columns:
                d["date"] = d.index
            d.date = d.date.apply(lambda dd: du.to_date(dd))
            d = d.reset_index(drop=True)
            return d

        return get_and_process_data

    @lru_cache()
    def _get_and_process_fundamentals_data(self):
        def get_and_process_data(securities, start_date, end_date):
            day_list = list(du.trade_range(start_date, end_date))
            max_count = 10000
            one_day_count = len(securities)
            max_days = max_count // one_day_count

            datas = []
            count = len(day_list)
            iter_count = math.ceil(count / max_days)
            for i in range(iter_count):
                request_count = min(max_days, count)
                e_date = day_list[count - 1]
                count -= max_days

                def get_fundamentals_data_2():
                    q = jq.query(
                        jq.valuation.turnover_ratio,
                        jq.valuation.pe_ratio,
                        jq.valuation.pe_ratio_lyr,
                        jq.valuation.pb_ratio,
                        jq.valuation.ps_ratio,
                        jq.valuation.pcf_ratio,
                    ).filter(jq.valuation.code.in_(securities))

                    return jq.get_fundamentals_continuously(q, e_date, count=request_count, panel=False)

                datas.insert(0, Wrapper._get_data(get_fundamentals_data_2))
            if len(datas) == 0:
                return pd.DataFrame(columns=['date', 'code', 'turn', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM'])
            d = pd.concat(datas)
            d = d.dropna()
            if "day" in d.columns:
                d.rename(columns={"day": "date"}, inplace=True)
            elif "date" not in d.columns:
                d["date"] = d.index

            d.date = d.date.apply(lambda dd: du.to_date(dd))
            d.rename(columns={
                "turnover_ratio": "turn",
                "pe_ratio": "peTTM",
                "pe_ratio_lyr": "peLYR",
                "pb_ratio": "pbMRQ",
                "ps_ratio": "psTTM",
                "pcf_ratio": "pcfNcfTTM",
            }, inplace=True)

            d = d.reset_index(drop=True)
            return d

        return get_and_process_data

    def get_cache(self, key):
        return self._cache.get(key)

    def put_cache(self, key, value):
        self._cache.set(key, value, self._cache_expire)

    @except_method(try_count=3)
    def get_cached_daily_data(self, *args, **kwargs):
        return super().get_cached_daily_data(*args, **kwargs)

    def get_price(self, security: str or list, start_date=None, end_date=None, frequency='daily', dtype=None,
                  fields=None, fq='pre', count=None, cached=False, cache_end=False, update_all=False, filter=True):
        if type(security) is str:
            security = [security]
        security = list(set(security))
        security = self.get_symbol(security)
        if not dtype or dtype is Type.INDEX or dtype is Type.STOCK:
            dtype = "stock"
            dtype_list = ["index", "stock"]
            key_prefix = None
        else:
            if type(dtype) is not list:
                dtype = [dtype]
            dtype = self._get_type(dtype, self.type_map)
            if type(dtype) is list:
                dtype = dtype[0]
            dtype_list = [dtype]
            key_prefix = dtype
        all_stocks = Wrapper._get_data(lambda: jq.get_all_securities(dtype_list))
        all_stock_list = list(all_stocks[all_stocks.type == dtype].index)
        all_securities = list(all_stocks.index)
        if len(set(security) & set(all_securities)) == 0:
            return pd.DataFrame(
                columns=['date', 'code', 'open', 'high', 'low', 'close', 'volume'])
        self._all_stocks = all_stocks
        self._all_securities = all_securities
        self._filter_securities = security if filter else all_stock_list
        start_date = du.to_date(start_date)
        end_date = du.to_date(end_date)
        if start_date is None:
            start_date = du.next_trade_day(end_date, 1 - count)

        gen_key = self._get_gen_price_key(frequency, fq, prefix=key_prefix)
        filter_stocks = self.filter_stocks
        get_and_process_data = self._get_and_process_price_data(frequency, fields, fq)
        all_data = self.get_cached_daily_data(
            start_date, end_date, gen_key, filter_stocks, get_and_process_data, cached, cache_end, update_all)

        if all_data is None:
            all_data = pd.DataFrame(
                columns=['date', 'code', 'open', 'high', 'low', 'close', 'volume'])
        return all_data

    def get_fund_price(self, security: str or list, start_date=None, end_date=None, frequency='daily',
                       fields=None, fq='pre', count=None, cached=False, cache_end=False, update_all=False, filter=True):
        if type(security) is str:
            security = [security]
        security = list(set(security))
        security = self.get_symbol(security)
        all_stocks = Wrapper._get_data(lambda: jq.get_all_securities(["fund"]))
        # all_stock_list = list(all_stocks[all_stocks.type == "stock"].index)
        all_stock_list = all_securities = list(all_stocks.index)
        if len(set(security) & set(all_securities)) == 0:
            return pd.DataFrame(
                columns=['date', 'code', 'open', 'high', 'low', 'close', 'volume'])
        self._all_stocks = all_stocks
        self._all_securities = all_securities
        self._filter_securities = security if filter else all_stock_list
        start_date = du.to_date(start_date)
        end_date = du.to_date(end_date)
        if start_date is None:
            start_date = du.next_trade_day(end_date, 1 - count)

        gen_key = self._get_gen_price_key(frequency, fq, prefix="fund")
        filter_stocks = self.filter_stocks
        get_and_process_data = self._get_and_process_price_data(frequency, fields, fq)
        all_data = self.get_cached_daily_data(
            start_date, end_date, gen_key, filter_stocks, get_and_process_data, cached, cache_end, update_all)

        if all_data is None:
            all_data = pd.DataFrame(
                columns=['date', 'code', 'open', 'high', 'low', 'close', 'volume'])
        return all_data

    def get_fund_data(self, security: str or list, start_date=None, end_date=None
                      , count=None, cached=False, cache_end=False, update_all=False, filter=True):

        if type(security) is str:
            security = [security]
        security = list(set(security))
        security = self.get_symbol(security)
        all_stocks = Wrapper._get_data(lambda: jq.get_all_securities(["stock"]))
        all_securities = list(all_stocks.index)

        self._all_stocks = all_stocks
        self._all_securities = all_securities
        self._filter_securities = security if filter else all_securities
        # if len(set(security) & set(all_securities)) == 0:
        #     return pd.DataFrame(
        #         columns=['date', 'code', 'turn', 'peTTM', 'peLYR', 'pbMRQ', 'psTTM', 'pcfNcfTTM'])
        start_date = du.to_date(start_date)
        end_date = du.to_date(end_date)
        if start_date is None:
            start_date = du.next_trade_day(end_date, 1 - count)

        gen_key = self._get_gen_fund_key()
        filter_stocks = self.filter_stocks
        get_and_process_data = self._get_and_process_fundamentals_data()
        all_data = self.get_cached_daily_data(
            start_date, end_date, gen_key, filter_stocks, get_and_process_data, cached, cache_end, update_all)

        if all_data is None:
            all_data = pd.DataFrame(columns=['date', 'code', 'turn', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM'])
        return all_data

    def get_type(self, type=[]):
        return self._get_type(type, Wrapper.type_map)

    def make_symbol(self, security):
        symbol_info = self.get_symbol_info(security, self.rev_cov)
        return self._make_symbol(symbol_info)

    def get_all_securities(self, dtype=[], date=None):
        if dtype is None:
            dtype = []
        if type(dtype) is not list:
            dtype = [dtype]
        t_dtype = self._get_type(dtype, self.type_map)
        ret = Wrapper._get_data(lambda: jq.get_all_securities(t_dtype, date))
        ret["start_date"] = ret["start_date"].apply(lambda d: du.to_date(d))
        ret["end_date"] = ret["end_date"].apply(lambda d: du.to_date(d))
        ret["type"] = self._rev_type(ret["type"], Wrapper.type_map)
        if "code" not in ret.columns:
            ret["code"] = ret.index
        ret["code"] = self.make_symbol(ret[["code", "type"]].values)
        if len(dtype) > 0:
            ret = ret[ret.type.isin(dtype)]
        return ret

    def get_file_postfix(self):
        return "jq"

    def __del__(self):
        jq.logout()

    def get_buy_code(self, code):
        return [c[:6] for c in code]

    def rev_cov(self, a: str or list, dtype="1"):
        if type(a) is not str and type(a) is not np.str and isinstance(a, Iterable):
            dtype = a[1]
            a: str = a[0]
        if type(a) is str or type(a) is np.str:
            num = a[:6] if '0' <= a[0] <= '9' else a[-6:]
            if num.startswith("399") or a.count(".XSHG") > 0 and num.startswith("0"):
                return num, Type.INDEX
            return num, dtype if type(dtype) is Type else self.rev_type_map[
                dtype] if dtype in self.rev_type_map else Type.STOCK
        raise ValueError()

    def rev_cov_str(self, a: str):
        if len(a) == 6:
            return a
        num = a[:6]

        if a.startswith("3") or a.startswith("0"):
            if a.endswith(".XSHG"):
                return num + Type.INDEX.value
            return num
        if a.startswith("5"):
            return num + Type.INDEX.value

    def get_index_weight(self, index, date=None):
        index = self.get_symbol(index)
        ret = Wrapper._get_data(lambda: jq.get_index_weights(index, date))
        if "code" not in ret.columns:
            ret["code"] = ret.index
        ret["code"] = self.make_symbol(ret["code"].values)
        ret = ret.reset_index(drop=True)
        return ret


if __name__ == "__main__":
    w = Wrapper()
    print(w.get_index_weight("000016.i")[:5])
    # print(w.get_price(["000001.XSHE", "000002.XSHE"], fields=["close"], count=10, end_date=dt.datetime.today()))
    # print(w.get_all_securities())
