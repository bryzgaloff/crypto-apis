import asyncio
import hmac
import json

import aiohttp

from .utils import coro, Ticker
from ..utils import normalize_keys, inverted, simplify_defaultdict


class InvalidResponseError(Exception):
    def __init__(self, response):
        self.response = response


class MarketAPI:
    API_ENDPOINT_TEMPLATE = '/{version}/{method}'
    DEFAULT_REQUEST_METHOD = None
    API_VERSION = None
    API_DOMAIN = None

    @classmethod
    async def ticker(cls, *args, **kwargs):
        return Ticker(await cls._fetch_ticker(*args, **kwargs))

    @classmethod
    def buy_prices(cls, from_currency, ticker=None):
        if ticker is None:
            return coro(cls.ticker(), cls.buy_prices, from_currency)
        return ticker.prices(from_currency)

    @classmethod
    def sell_prices(cls, to_currency, ticker=None):
        if ticker is None:
            return coro(cls.ticker(), cls.sell_prices, to_currency)
        return ticker.inverted().prices(to_currency)

    @classmethod
    def buy_cost(cls, balance, target_currency, ticker=None):
        buy_prices = cls.buy_prices(target_currency, ticker)
        if asyncio.iscoroutine(buy_prices):
            return coro(buy_prices, cls.buy_cost, balance, target_currency)
        return balance * buy_prices

    @classmethod
    def sell_cost(cls, balance, target_currency, ticker=None):
        sell_prices = cls.sell_prices(target_currency, ticker)
        if asyncio.iscoroutine(sell_prices):
            return coro(sell_prices, cls.sell_cost, balance, target_currency)
        return balance * sell_prices

    @classmethod
    async def pairs_as_symbols(cls, *args, **kwargs):
        data = await cls._get_pairs_as_symbols_data(*args, **kwargs)
        return normalize_keys(simplify_defaultdict(data), recursive=True)

    @classmethod
    async def _fetch_ticker(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    async def _get_pairs_as_symbols_data(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    async def _make_request(
            cls, api_method_name, params=None, headers=None, request_method=None
    ):
        api_endpoint = 'https://{domain}{endpoint}'.format(
            domain=cls.API_DOMAIN,
            endpoint=cls.API_ENDPOINT_TEMPLATE.format(
                version=cls.API_VERSION,
                method=api_method_name
            )
        )

        request_method = request_method or cls.DEFAULT_REQUEST_METHOD

        headers = headers or {}
        headers.setdefault('Content-type', 'application/x-www-form-urlencoded')

        kwargs = {'headers': headers}
        if request_method == 'POST':
            kwargs['data'] = params
        else:
            kwargs['params'] = params

        async with aiohttp.ClientSession() as session:
            async with session.request(request_method, api_endpoint, **kwargs) \
                    as response:
                response_content = await response.read()
                try:
                    obj = json.loads(response_content.decode('utf-8'))
                    return obj
                except json.decoder.JSONDecodeError:
                    print('Error while parsing response:', response_content)
                    raise InvalidResponseError(response)


class ExchangeAPI(MarketAPI):
    SIGNATURE_DIGEST_MOD = None

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = bytes(api_secret, encoding='utf-8')

    async def balances(self, *args, **kwargs):
        balances_data = await self._get_balances_data(*args, **kwargs)
        for entry in balances_data.values():
            normalize_keys(entry, inplace=True)
        return balances_data

    async def open_orders(self, *args, **kwargs):
        raw_data = await self._get_open_orders_data(*args, **kwargs)
        return normalize_keys(simplify_defaultdict(raw_data), recursive=True)

    def open_buy_orders(self, to_currency, open_orders=None):
        if open_orders is None:
            return coro(self.open_orders(), self.open_buy_orders, to_currency)
        inverted_orders = inverted(
            open_orders, lambda orders: list(
                dict(
                    order_id=order['order_id'],
                    amount=order['amount'] * order['price'],
                    price=order['price'] and 1 / order['price']
                )
                for order in orders
            )
        )
        return inverted_orders.get(to_currency, {})

    def open_sell_orders(self, from_currency, open_orders=None):
        if open_orders is None:
            return \
                coro(self.open_orders(), self.open_sell_orders, from_currency)
        return open_orders.get(from_currency, {})

    async def _signed_api_query(self, *args, **kwargs):
        raise NotImplementedError

    def _sign(self, data):
        h = hmac.new(key=self.api_secret, digestmod=self.SIGNATURE_DIGEST_MOD)
        h.update(data.encode('utf-8'))
        return h.hexdigest()

    async def _get_balances_data(self, *args, **kwargs):
        raise NotImplementedError

    async def _get_open_orders_data(self, *args, **kwargs):
        raise NotImplementedError
