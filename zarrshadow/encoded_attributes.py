"""Decode the attributes of a node, whichever of the attribute conventions it registers.

zarrshadow writes NaN and infinite attributes following the Non-Finite
Attributes convention (zarrshadow.non_finite), and bytes following the Bytes
Attributes convention (zarrshadow.bytes_attributes). The two can be decoded in
either order.
"""

from __future__ import annotations

from collections.abc import Mapping

from . import bytes_attributes, non_finite


def decode_attributes(attributes: Mapping) -> dict:
    """The attributes of a group or array, decoded for each attribute convention it registers.

    Pass the attrs of a zarr group or array. The entries that register these
    conventions are left out of the result, and attributes that register
    neither are returned as they are.
    """
    return bytes_attributes.decode_attributes(non_finite.decode_attributes(attributes))
