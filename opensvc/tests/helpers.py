def assert_resource_has_mandatory_methods(resource):
    for method in ['start', 'stop']:
        assert callable(getattr(resource, method))
