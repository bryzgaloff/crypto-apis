from collections import defaultdict, Counter, Iterable


def simplify_defaultdict(d):
    if isinstance(d, defaultdict):
        d = {k: simplify_defaultdict(v) for k, v in d.items()}
    return d


def to_args(obj):
    if isinstance(obj, str):
        tuple_ = (obj, )
        return tuple_
    if isinstance(obj, Iterable):
        return tuple(obj)
    if obj is None:
        return ()
    raise ValueError('obj argument has to be str or iterable')


ALIASES = {
    ('BCC', 'BCH', 'Bitcoin Cash (BCH)'): 'BCH',
    ('DASH', 'DSH', 'Dash (DASH)'): 'DASH',
    ('Bitcoin (BTC)', 'BTC'): 'BTC',
    ('Ethereum (ETH)', 'ETH'): 'ETH',
    ('Zcash (ZEC)', 'ZEC'): 'ZEC',
    ('Monero (XMR)', 'XMR'): 'XMR',
    ('Litecoin (LTC)', 'LTC'): 'LTC',
    ('Ether Classic (ETC)', 'ETC'): 'ETC',
    ('Namecoin (NMC)', 'NMC'): 'NMC',
    ('Peercoin (PPC)', 'PPC'): 'PPC',
    ('Ripple (XRP)', 'XRP'): 'XRP',
    ('Dogecoin (DOGE)', 'DOGE'): 'DOGE',
    ('RUR', 'RUB'): 'RUB',
    ('Tether (USDT)', 'USDT'): 'USDT',
    ('NEM (XEM)', 'XEM'): 'XEM',
    ('Augur (REP)', 'REP'): 'REP'
}
_ALIASES_FLATTENED = {src: trg for key, trg in ALIASES.items() for src in key}


def normalized_key(k):
    u = k.upper()
    return _ALIASES_FLATTENED.get(k) or _ALIASES_FLATTENED.get(u) or u


def _normalize_keys_impl(d, *, agg=None, inplace=False):
    result = {}
    if agg is None:
        for key, value in d.items():
            key = normalized_key(key)
            if key in result:
                raise ValueError(
                    'Multiple values for normalized key "{}" are found, '
                    'but no aggregation is provided'.format(key)
                )
            result[key] = value
    else:
        result = defaultdict(list)
        for key, value in d.items():
            result[normalized_key(key)].append(value)
        for key, values in result.items():
            result[key] = agg(values)
    if inplace:
        d.clear()
        d.update(result)
        return d
    return result


def normalize_keys(d, *, agg=None, inplace=False, recursive=False):
    """
    Normalizes dict keys. If key or key.upper() is found in ALIASES then the key
        is renamed to corresponding ALIASES value else the key is just upper'ed.
    :param dict d: the dict to be modified
    :param callable agg: an aggregation function used to reduce multiple values
        found for the same key or its alias; agg=None by default => raises error
        if multiple values are found
    :param bool inplace: if False then creates normalized copy
        else normalizes inplace; inplace option also saves type of modifying obj
    :param bool recursive: if True then all values-dicts will also be normalized
    :return: the modified dict
    """
    if not recursive:
        return _normalize_keys_impl(d, agg=agg, inplace=inplace)
    if isinstance(d, dict):
        if inplace:
            for entry in d.values():
                normalize_keys(entry, agg=agg, inplace=True)
        else:
            d = {
                key: normalize_keys(entry, agg=agg)
                for key, entry in d.items()
            }
        return _normalize_keys_impl(d, agg=agg, inplace=inplace)
    return d


def inverted(d, inverter=lambda x: x):
    """
    Inverts 2-level dict: [from_][to] -> [to][from_]
    :param inverter: function to invert values
    :param dict d: initial dict
    :rtype: dict
    :return: inverted dict
    """
    result = defaultdict(dict)
    for from_, entry in d.items():
        for to, value in entry.items():
            result[to][from_] = inverter(value)
    return dict(result)


def update(to, from_, level=None):
    """
    Recursive dict update
    :param dict to:
    :param dict from_:
    :param int level: None => update infinitely recursively;
        else update up to specified level
    """
    if not isinstance(to, dict):
        return
    if level == 1:
        to.update(from_)
    else:
        existing_keys = set(to)
        new_keys = set(from_)
        for key in new_keys - existing_keys:  # right-only
            to[key] = from_[key]
        for key in new_keys & existing_keys:  # intersection
            update(
                to[key], from_[key],
                level=level and level - 1
            )


class Balance(Counter):
    def __mul__(self, other):
        if isinstance(other, Counter):
            result = Balance()
            for key, value in self.items():
                result[key] = value * other.get(key, 0)
            for key in other:
                if key not in result:
                    result[key] = 0
            return result
        elif isinstance(other, (int, float)):
            result = Balance(self)
            for key in result:
                result[key] *= other
            return result
        else:
            raise NotImplementedError()

    def __rmul__(self, other):
        try:
            return self * other
        except NotImplementedError:
            raise

    def __add__(self, other):
        return Balance(super().__add__(other))

    def sum(self):
        return sum(self.values())

    def project(self, *keys):
        return Balance({k: v for k, v in self.items() if k in keys})

    def excluding(self, *keys):
        return Balance({k: v for k, v in self.items() if k not in keys})

    def normalize(self, *, agg=None, inplace=False):
        if inplace:
            return normalize_keys(self, agg=agg, inplace=True)
        return Balance(normalize_keys(self, agg=agg))
