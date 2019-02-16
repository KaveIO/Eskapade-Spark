try:
    import jaydebeapi
except ImportError:
    from eskapadespark.exceptions import MissingJayDeBeApiError
    raise MissingJayDeBeApiError()
