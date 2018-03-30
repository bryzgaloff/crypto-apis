import asyncio
import hashlib
import time
import urllib.parse
from collections import defaultdict

from ..utils import Balance, normalized_key
from .utils import Ticker
from .spec import ExchangeAPI, MarketAPI, InvalidResponseError
from .wrappers import best_change as bc_wrapper

__all__ = ('BinanceAPI', 'ExmoAPI', 'BestChangeAPI', 'YobitAPI')


class BinanceAPI(ExchangeAPI):
    API_DOMAIN = 'api.binance.com'
    API_ENDPOINT_TEMPLATE = '/api/{method}'
    SIGNATURE_DIGEST_MOD = hashlib.sha256
    DEFAULT_REQUEST_METHOD = 'GET'

    async def _signed_api_query(self, api_method, params=None, **kwargs):
        if params is None:
            params = {}
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = self._sign(urllib.parse.urlencode(params))
        headers = {'X-MBX-APIKEY': self.api_key}
        return await self._make_request(api_method, params, headers, **kwargs)

    @classmethod
    async def _symbols_as_pairs(cls):
        response = await cls._make_request('v1/exchangeInfo')
        result = {}
        for symbol in response['symbols']:
            symbol_name = symbol['symbol']
            result[symbol_name] = (symbol['baseAsset'], symbol['quoteAsset'])
        return result

    @classmethod
    async def _fetch_ticker(cls):
        pairs, response = await asyncio.gather(
            cls._symbols_as_pairs(),
            cls._make_request('v1/ticker/24hr')
        )
        result = defaultdict(dict)
        for entry in response:
            symbol_name = entry['symbol']
            from_, to = pairs[symbol_name]
            sell_price = float(entry['bidPrice'])
            result[from_][to] = sell_price and 1 / sell_price
            result[to][from_] = float(entry['askPrice'])
        return Ticker(result)

    async def _get_balances_data(self):
        def update_balances(key, entry_):
            value = float(entry_[key])
            if value > 0:
                currency = entry_['asset']
                balances[key][currency] += value

        balances = {'free': Balance(), 'locked': Balance()}
        response = await self._signed_api_query('v3/account')
        raw_balances = response['balances']
        for entry in raw_balances:
            update_balances('free', entry)
            update_balances('locked', entry)
        balances['total'] = balances['free'] + balances['locked']

        for balance in balances.values():
            balance.normalize(inplace=True)

        return balances

    async def _get_open_orders_data(self):
        pairs, response = await asyncio.gather(
            self._symbols_as_pairs(),
            self._signed_api_query('v3/openOrders')
        )
        result = defaultdict(lambda: defaultdict(list))
        for order in response:
            symbol = order['symbol']
            from_, to = pairs[symbol]
            price = float(order['price'])
            amount = float(order['origQty'])
            if order['side'] == 'BUY':
                from_, to = to, from_
            else:
                price = 1 / price
                amount = amount / price
            result[from_][to].append({
                'amount': amount,
                'price': price,
                'order_id': order['orderId']
            })
        return result


class ExmoAPI(ExchangeAPI):
    API_DOMAIN = 'api.exmo.me'
    API_VERSION = 'v1'
    SIGNATURE_DIGEST_MOD = hashlib.sha512
    DEFAULT_REQUEST_METHOD = 'POST'

    async def _signed_api_query(self, api_method, params=None, **kwargs):
        if params is None:
            params = {}
        params['nonce'] = int(round(time.time() * 1000))
        headers = {
            'Key': self.api_key,
            'Sign': self._sign(urllib.parse.urlencode(params))
        }
        return await self._make_request(api_method, params, headers, **kwargs)

    @classmethod
    async def _fetch_ticker(cls):
        response = await cls._make_request('ticker')
        result = defaultdict(dict)
        for pair, prices in response.items():
            from_, to = pair.split('_')
            buy_price = float(prices['buy_price'])
            result[from_][to] = buy_price and 1 / buy_price
            result[to][from_] = float(prices['sell_price'])
        return Ticker(result)

    async def _get_balances_data(self):
        def parse_and_filter(d):
            parsed = map(lambda pair: (pair[0], float(pair[1])), d.items())
            filtered = filter(lambda pair: pair[1] > 0, parsed)
            return Balance(dict(filtered)).normalize()

        response = await self._signed_api_query('user_info')
        free = parse_and_filter(response.get('balances'))
        locked = parse_and_filter(response.get('reserved'))
        return {
                'free': free,
                'locked': locked,
                'total': free + locked
            }

    async def _get_open_orders_data(self):
        response = await self._signed_api_query('user_open_orders')
        result = defaultdict(lambda: defaultdict(list))
        for orders in response.values():
            for order in orders:
                from_, to = order['pair'].split('_')
                price = float(order['price'])
                if order['type'] == 'buy':
                    from_, to = to, from_
                    amount = float(order['quantity'])
                else:  # 'sell' type
                    price = 1 / price
                    amount = float(order['amount'])
                result[from_][to].append({
                    'amount': amount,
                    'price': price,
                    'order_id': order['order_id']
                })
        return result


class BestChangeAPI(MarketAPI):
    @classmethod
    async def _fetch_ticker(cls):
        result = defaultdict(dict)
        for row in bc_wrapper.load_ticker_data():  # TODO make async
            from_currency, to_currency, _, give, receive = row
            result[from_currency][to_currency] = give / receive
        return Ticker(result)


class YobitAPI(ExchangeAPI):
    API_DOMAIN = 'yobit.io'
    API_ENDPOINT_TEMPLATE = '/{method}'
    SIGNATURE_DIGEST_MOD = hashlib.sha512
    DEFAULT_REQUEST_METHOD = 'POST'
    _ticker_cache = defaultdict(dict)

    @classmethod
    async def _fetch_ticker(cls, *pairs):
        await cls.fetch_prices(*pairs)
        return cls._ticker_cache

    async def _signed_api_query(self, api_method, params=None, **kwargs):
        if params is None:
            params = {}
        params['nonce'] = int(time.time())
        params['method'] = api_method
        headers = {
            'Key': self.api_key,
            'Sign': self._sign(urllib.parse.urlencode(params))
        }
        return await self._make_request('tapi', params, headers, **kwargs)

    async def _get_balances_data(self):
        response = await self._signed_api_query('getInfo')
        free_balance = Balance(response['return']['funds'])
        total_balance = Balance(response['return']['funds_incl_orders'])
        return {
            'free': free_balance,
            'total': total_balance,
            'locked': Balance(total_balance - free_balance)
        }

    async def _get_open_orders_data(self, pair):
        response = await self._signed_api_query('ActiveOrders', {'pair': pair})
        result = defaultdict(lambda: defaultdict(list))
        for order_id, order in response['return'].items():
            from_, to = order['pair'].split('_')
            price = order['rate']
            amount = order['amount']
            if order['type'] == 'buy':
                from_, to = to, from_
            else:
                price = 1 / price
                amount = amount / price
            result[from_][to].append({
                'amount': amount,
                'price': price,
                'order_id': order_id
            })
        return result

    @classmethod
    async def _get_pairs_as_symbols_data(cls):
        response = await cls._make_request('api/3/info')
        pairs = defaultdict(dict)
        for symbol in response['pairs']:
            first, second = map(lambda x: x.upper(), symbol.split('_'))
            pairs[first][second] = pairs[second][first] = symbol
        return pairs

    @classmethod
    async def fetch_prices(cls, *pairs):
        """
        Load prices to YobitAPI._ticker_cache.
        :param pairs: iterable of pairs, e.g. ('BTC', 'LTC')
        """
        pairs_dict = await cls.pairs_as_symbols()
        symbols = '-'.join(
            pairs_dict[first][second]
            for first, second in pairs if first != second
            and first in pairs_dict and second in pairs_dict[first]
        )
        if not symbols:
            return
        ticker_data = await cls._make_request('api/3/ticker/{}'.format(symbols))
        for symbol, entry in ticker_data.items():
            to, from_ = map(normalized_key, symbol.split('_'))
            cls._ticker_cache[from_][to] = entry['sell']
            cls._ticker_cache[to][from_] = entry['buy'] and 1 / entry['buy']

    @classmethod
    async def fetch_prices_by_balance(cls, balance, target_currency):
        pairs = ((target_currency, currency) for currency in balance)
        await cls.fetch_prices(*pairs)

    @classmethod
    def buy_cost(cls, balance, target_currency, ticker=None):
        if ticker is None:
            return cls._buy_cost_coro(balance, target_currency)
        return balance * cls.buy_prices(target_currency, ticker)

    @classmethod
    def sell_cost(cls, balance, target_currency, ticker=None):
        if ticker is None:
            return cls._sell_cost_coro(balance, target_currency)
        return balance * cls.sell_prices(target_currency, ticker)

    @classmethod
    async def _buy_cost_coro(cls, balance, target_currency):
        await cls.fetch_prices_by_balance(balance, target_currency)
        return balance * \
            cls.buy_prices(target_currency, Ticker(cls._ticker_cache))

    @classmethod
    async def _sell_cost_coro(cls, balance, target_currency):
        await cls.fetch_prices_by_balance(balance, target_currency)
        return balance * \
            cls.sell_prices(target_currency, Ticker(cls._ticker_cache))
