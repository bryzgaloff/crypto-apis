from .spec import ExplorerAPI

__all__ = (
    'BlockchainAPI', 'BlockExplorerAPI', 'CashExplorerBitcoinAPI', 'ChainsoAPI',
    'EthplorerAPI', 'ExplorerDashAPI', 'GasTrackerAPI'
)


class BlockchainAPI(ExplorerAPI):
    BASE_URL = 'https://blockchain.info/ru'
    SINGLE_ADDRESS_ENDPOINT = 'balance?active={address}'
    MULTI_ADDRESS_ENDPOINT = 'balance?active={multi_address}'
    MULTI_ADDRESS_SEPARATOR = '|'
    DEFAULT_CURRENCY = 'BTC'
    BALANCE_DIVISOR = 10 ** 8

    @classmethod
    def _extract_balance(cls, response):
        return sum(entry['final_balance'] for entry in response.values())


class BlockExplorerAPI(ExplorerAPI):
    BASE_URL = 'https://blockexplorer.com/api'
    SINGLE_ADDRESS_ENDPOINT = 'addr/{address}/balance'
    DEFAULT_CURRENCY = 'BTC'
    BALANCE_DIVISOR = 10 ** 8


class CashExplorerBitcoinAPI(ExplorerAPI):
    BASE_URL = 'https://cashexplorer.bitcoin.com/api'
    SINGLE_ADDRESS_ENDPOINT = 'addr/{address}/balance'
    DEFAULT_CURRENCY = 'BCH'
    BALANCE_DIVISOR = 10 ** 8


class ChainsoAPI(ExplorerAPI):
    BASE_URL = 'https://chain.so/api/v2'
    SINGLE_ADDRESS_ENDPOINT = 'get_address_balance/{currency}/{address}'
    DEFAULT_CURRENCY = 'LTC'  # also available: DOGE, BTC

    @classmethod
    def _extract_currency(cls, response):
        network = response['data']['network']
        if network.endswith('TEST'):
            return network[:-4]
        return network

    @classmethod
    def _extract_balance(cls, response):
        return float(response['data']['confirmed_balance'])


class EthplorerAPI(ExplorerAPI):
    BASE_URL = 'https://api.ethplorer.io'
    SINGLE_ADDRESS_ENDPOINT = 'getAddressInfo/{address}?apiKey={api_key}'
    BASIC_API_KEY = 'freekey'
    DEFAULT_CURRENCY = 'ETH'

    @classmethod
    def _extract_balance(cls, response):
        return response['ETH']['balance']


class ExplorerDashAPI(ExplorerAPI):
    BASE_URL = 'https://explorer.dash.org'
    SINGLE_ADDRESS_ENDPOINT = 'chain/Dash/q/addressbalance/{address}'
    DEFAULT_CURRENCY = 'DASH'


class GasTrackerAPI(ExplorerAPI):
    BASE_URL = 'https://api.gastracker.io'
    SINGLE_ADDRESS_ENDPOINT = 'v1/addr/{address}'
    DEFAULT_CURRENCY = 'ETC'
    BALANCE_DIVISOR = 10 ** 18

    @classmethod
    def _extract_balance(cls, response):
        return int(response['balance']['wei'])
