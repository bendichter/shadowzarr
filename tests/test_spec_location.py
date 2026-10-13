"""Finding the group that holds an NWB file's cached specifications."""

import h5py
import numpy as np
import pytest

from shadowzarr.hdf5 import _spec_location


def _write(path, location):
    with h5py.File(path, "w") as f:
        f["data"] = np.arange(3)
        if location is not None:
            group = f.require_group(location)
            group.create_group("core")
            f.attrs[".specloc"] = group.ref


@pytest.mark.parametrize("location", ["specifications", "elsewhere/specs"])
def test_spec_location(tmp_path, location):
    path = tmp_path / "file.h5"
    _write(path, location)
    with h5py.File(path, "r") as f:
        assert _spec_location(f) == location


def test_usual_location_does_not_search_the_file(tmp_path, monkeypatch):
    """The .name of an object opened by reference searches the whole file, so it is avoided."""
    path = tmp_path / "file.h5"
    _write(path, "specifications")

    def refuse(self):
        raise AssertionError("searched the file for the name")

    monkeypatch.setattr(h5py.Group, "name", property(refuse))
    with h5py.File(path, "r") as f:
        assert _spec_location(f) == "specifications"


def test_no_spec_location(tmp_path):
    path = tmp_path / "file.h5"
    _write(path, None)
    with h5py.File(path, "r") as f:
        assert _spec_location(f) is None
