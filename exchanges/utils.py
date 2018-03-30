from ..utils import normalize_keys, Balance, inverted


def coro(awaitable, converter, *args):
    async def _coro():
        awaited = await awaitable
        return converter(*args, awaited)
    return _coro()


class Ticker(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._normalize_keys()
        self._add_self_prices()

    def _add_self_prices(self):
        for key in self:
            self[key][key] = 1.0

    def _normalize_keys(self):
        normalize_keys(self, inplace=True, recursive=True)

    def prices(self, target_currency):
        return Balance(self[target_currency])

    def inverted(self):
        return Ticker(inverted(self, lambda price: price and 1 / price))

    def increased_by_fee(self, fee):
        if fee < 1:
            fee += 1
        result = Ticker()
        for outer_key, entry in self.items():
            new_entry = {}
            for inner_key, value in entry.items():
                if inner_key == outer_key:
                    new_entry[inner_key] = value
                else:
                    new_entry[inner_key] = value * fee
            result[outer_key] = new_entry
        return result

    def projected(self, accepted_keys):
        accepted_keys = set(accepted_keys)
        return Ticker({
            currency: {
                currency: price
                for currency, price in prices.items()
                if currency in accepted_keys
            }
            for currency, prices in self.items()
            if currency in accepted_keys
        })
