try:
    # noinspection PyCompatibility
    from unittest.mock import call
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import call


try:
    # noinspection PyCompatibility
    from unittest.mock import ANY
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import ANY


def assert_resource_has_mandatory_methods(resource):
    for method in ['start', 'stop']:
        assert callable(getattr(resource, method))
