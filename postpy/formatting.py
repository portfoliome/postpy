"""Formatting helpers."""

from types import MappingProxyType

PYFORMAT = 'pyformat'
NAMED_STYLE = 'named_style'


def pyformat_parameters(parameters):
    return ', '.join(['%s']*len(parameters))


def named_style_parameters(parameters):
    return ', '.join('%({})s'.format(p) for p in parameters)


PARAM_STYLES = MappingProxyType({
    PYFORMAT: pyformat_parameters,
    NAMED_STYLE: named_style_parameters
})
