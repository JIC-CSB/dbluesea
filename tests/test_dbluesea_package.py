"""Test the dbluesea package."""


def test_version_is_string():
    import dbluesea
    assert isinstance(dbluesea.__version__, str)
