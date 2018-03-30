import asyncio
import json

import aiohttp

from ..utils import to_args, Balance


class ExplorerAPI:
    BASE_URL = None
    BASIC_API_KEY = None
    DEFAULT_CURRENCY = None
    SINGLE_ADDRESS_ENDPOINT = None
    MULTI_ADDRESS_ENDPOINT = None
    MULTI_ADDRESS_SEPARATOR = None
    BALANCE_DIVISOR = None  # to convert sat -> btc/bch, wei -> eth/etc

    @classmethod
    async def balance(cls, *addresses_, addresses=None, **endpoint_substitutions):
        addresses = addresses_ or to_args(addresses)
        cls._enrich_substitutions(endpoint_substitutions)
        if len(addresses) > 1:
            return await cls._get_multi_address_balance(
                    addresses, endpoint_substitutions
                )
        elif addresses:
            return await cls._get_single_address_balance(
                    addresses[0], endpoint_substitutions
                )

    @classmethod
    async def _get_single_address_balance(cls, address, endpoint_substitutions):
        endpoint = cls.SINGLE_ADDRESS_ENDPOINT.format(
            address=address,
            **endpoint_substitutions
        )
        return await cls._request_balance(endpoint)

    @classmethod
    async def _get_multi_address_balance(cls, addresses, endpoint_substitutions):
        if cls.MULTI_ADDRESS_ENDPOINT is None:  # naive implementation
            balance = Balance()
            amounts = await asyncio.gather(*(
                cls._get_single_address_balance(address, endpoint_substitutions)
                for address in addresses
            ))
            for amount in amounts:
                balance += amount
            return balance

        # using special endpoints
        if cls.MULTI_ADDRESS_SEPARATOR is None:
            raise RuntimeError('No multi address separator is provided')
        endpoint = cls.MULTI_ADDRESS_ENDPOINT.format(
            multi_address=cls.MULTI_ADDRESS_SEPARATOR.join(addresses),
            **endpoint_substitutions
        )
        return await cls._request_balance(endpoint)

    @classmethod
    async def _request_balance(cls, endpoint):
        url = '{}/{}'.format(cls.BASE_URL.rstrip('/'), endpoint.lstrip('/'))
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    response_json = json.loads(await response.read())
                    currency = cls._extract_currency(response_json)
                    balance = cls._extract_balance(response_json)
                    if cls.BALANCE_DIVISOR:
                        balance /= cls.BALANCE_DIVISOR
                    return Balance({currency: balance})
                else:
                    response.raise_for_status()

    @classmethod
    def _extract_balance(cls, response):
        if response is None:
            return None
        balance = float(response)
        return balance

    @classmethod
    def _extract_currency(cls, response):
        if cls.DEFAULT_CURRENCY is None:
            raise NotImplementedError('Default currency has to be specified')
        return cls.DEFAULT_CURRENCY

    @classmethod
    def _enrich_substitutions(cls, substitutions):
        substitutions.setdefault('api_key', cls.BASIC_API_KEY)
        if cls.DEFAULT_CURRENCY is not None:
            substitutions.setdefault('currency', cls.DEFAULT_CURRENCY)
