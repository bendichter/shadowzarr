"""Attributes that are bytes, stored with the Bytes Attributes convention."""

import json
import math

import numpy as np
import pytest

from shadowzarr import RfsBuilder, decode_attributes, open_rfs, write_rfs
from shadowzarr import non_finite
from shadowzarr.bytes_attributes import CONVENTION, decode, encode, encode_attributes

NAN = float("nan")

# (decoded value, encoded value), as in the convention's test vectors
VECTORS = [
    (b"", "base64:"),
    (b"\x00\xff", "base64:AP8="),
    (b"abc", "base64:YWJj"),
    (b"hello", "base64:aGVsbG8="),
    ("base64:AP8=", "_str_base64:AP8="),
    ("base64:", "_str_base64:"),
    ("base64:not base64!", "_str_base64:not base64!"),
    ("_str_base64:AP8=", "_str__str_base64:AP8="),
    ("base64", "base64"),
    ("Base64:AP8=", "Base64:AP8="),
    (" base64:AP8=", " base64:AP8="),
    ("_str_hello", "_str_hello"),
    ("NaN", "NaN"),
    ("_str_NaN", "_str_NaN"),
    ("", ""),
    (1.5, 1.5),
    (None, None),
    (True, True),
]
INVALID = ["base64:AP8", "base64:AP9=", "base64:A===", "base64:AP8= ", "base64:-_8=", "base64:A"]


def _strict(text):
    """Parse JSON, refusing the bare tokens NaN, Infinity, and -Infinity."""

    def refuse(token):
        raise ValueError(f"{token} is not valid JSON")

    return json.loads(text, parse_constant=refuse)


@pytest.mark.parametrize("decoded, encoded", VECTORS)
def test_vectors(decoded, encoded):
    assert encode(decoded) == encoded
    result = decode(encoded)
    assert type(result) is type(decoded) and result == decoded


@pytest.mark.parametrize("encoded", INVALID)
def test_invalid_payloads_are_rejected(encoded):
    with pytest.raises(ValueError):
        decode(encoded)


def test_attributes_are_encoded_only_where_a_value_is_bytes():
    plain = {"text": "base64:AP8=", "rate": 30000.0}
    assert encode_attributes(plain) == plain
    assert decode_attributes(plain) == plain  # not registered, so the string is text

    other = {"uuid": "00000000-0000-0000-0000-000000000000"}
    stored = encode_attributes({"digest": b"\x00\xff", "text": "base64:AP8=", "zarr_conventions": [other]})
    assert stored == {"digest": "base64:AP8=", "text": "_str_base64:AP8=", "zarr_conventions": [other, CONVENTION]}
    assert encode_attributes(stored) == stored  # already registered
    assert decode_attributes(stored) == {"digest": b"\x00\xff", "text": "base64:AP8=", "zarr_conventions": [other]}


def test_with_non_finite_attributes_in_either_order():
    value = {"resolution": NAN, "digest": b"\x10", "a": "NaN", "b": "base64:Q"}
    bytes_first = non_finite.encode_attributes(encode_attributes(value))
    nan_first = encode_attributes(non_finite.encode_attributes(value))
    for stored in (bytes_first, nan_first):
        assert {k: v for k, v in stored.items() if k != "zarr_conventions"} == {
            "resolution": "NaN",
            "digest": "base64:EA==",
            "a": "_str_NaN",
            "b": "_str_base64:Q",
        }
        decoded = decode_attributes(stored)
        assert math.isnan(decoded.pop("resolution"))
        assert decoded == {"digest": b"\x10", "a": "NaN", "b": "base64:Q"}


def test_builder_encodes_bytes(tmp_path):
    builder = RfsBuilder()
    builder.add_group("", {"header": b"\x89HDF\r\n\x1a\n", "name": "a"})
    builder.add_group("plain", {"note": "base64:AP8="})
    builder.add_array(
        "data", shape=[2], data_type="float64", chunk_shape=[2],
        attributes={"resolution": NAN, "digest": [b"\x00", b"\xff"], "note": "base64:"},
    )
    rfs = builder.build()

    root = _strict(rfs["refs"]["zarr.json"])["attributes"]
    assert root == {"header": "base64:iUhERg0KGgo=", "name": "a", "zarr_conventions": [CONVENTION]}
    data = _strict(rfs["refs"]["data/zarr.json"])["attributes"]
    assert data["digest"] == ["base64:AA==", "base64:/w=="] and data["note"] == "_str_base64:"
    assert data["resolution"] == "NaN"
    assert data["zarr_conventions"] == [CONVENTION, non_finite.CONVENTION]
    # a node with no bytes is left as it was
    assert _strict(rfs["refs"]["plain/zarr.json"])["attributes"] == {"note": "base64:AP8="}

    for location in (rfs, str(tmp_path / "a.shadowzarr")):
        if isinstance(location, str):
            write_rfs(rfs, location)
        group = open_rfs(location)
        assert decode_attributes(group.attrs) == {"header": b"\x89HDF\r\n\x1a\n", "name": "a"}
        attributes = decode_attributes(group["data"].attrs)
        assert math.isnan(attributes.pop("resolution"))
        assert attributes == {"digest": [b"\x00", b"\xff"], "note": "base64:"}
        assert decode_attributes(group["plain"].attrs) == {"note": "base64:AP8="}


def test_hdf5_attributes(tmp_path):
    """Attributes that are not valid UTF-8, and opaque data, are kept; they used to be dropped."""
    h5py = pytest.importorskip("h5py")
    from shadowzarr import generate_rfs

    latin1 = "Müller".encode("latin-1")
    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as f:
        dataset = f.create_dataset("data", data=np.arange(4.0))
        dataset.attrs["operator"] = np.bytes_(latin1)  # fixed-length, not valid UTF-8
        dataset.attrs["labels"] = np.array([b"ok", latin1])
        dataset.attrs["digest"] = np.void(b"\x00\xffabc")  # opaque
        dataset.attrs["opaque_text"] = np.void(b"hello")  # opaque, but valid UTF-8: still bytes
        dataset.attrs["blocks"] = np.array([np.void(b"\x01\x02"), np.void(b"\x03\x04")])
        dataset.attrs["note"] = "base64:AP8="
        dataset.attrs["text"] = np.bytes_(b"plain ascii")  # valid UTF-8, so text as before
        f.create_dataset("plain", data=np.arange(4.0)).attrs["note"] = "base64:AP8="

    rfs = generate_rfs(str(path))
    stored = _strict(rfs["refs"]["data/zarr.json"])["attributes"]
    assert stored["operator"] == "base64:TfxsbGVy"
    assert stored["note"] == "_str_base64:AP8=" and stored["text"] == "plain ascii"
    assert CONVENTION in stored["zarr_conventions"]
    plain = _strict(rfs["refs"]["plain/zarr.json"])["attributes"]
    assert plain["note"] == "base64:AP8=" and "zarr_conventions" not in plain

    attributes = decode_attributes(open_rfs(rfs)["data"].attrs)
    assert attributes["operator"] == latin1
    assert attributes["labels"] == ["ok", latin1]
    assert attributes["digest"] == b"\x00\xffabc"
    assert attributes["opaque_text"] == b"hello"
    assert attributes["blocks"] == [b"\x01\x02", b"\x03\x04"]
    assert attributes["note"] == "base64:AP8=" and attributes["text"] == "plain ascii"
