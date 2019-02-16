import jaydebeapi

from escore.core.dbconnection import DbConnection

def _to_date(rs, col):
    """Fix date-to-string conversion in JayDeBeApi

    Dates before the year 1900 are not correctly converted to strings by
    JayDeBeApi.  This is caused by an exception in
    datetime.datetime.strftime.  The conversion is fixed with this helper
    function.
    """

    java_val = rs.getDate(col)
    if not java_val:
        return None
    return str(java_val)[:10]

# fix date-to-string conversion in JayDeBeApi
try:
    jaydebeapi._DEFAULT_CONVERTERS['DATE'] = _to_date
except NameError:
    pass

class JdbcConnection(DbConnection):
    """Process service for managing a JDBC connection"""

    def create_connection(self):
        """Create JayDeBeApi connection instance"""

        if self._conn:
            raise RuntimeError('JDBC connection already exists.')

        # get configuration
        cfg = self.get_config()

        # get connection properties
        dr_jar = cfg.get(self.config_section, 'driver_jar')
        dr_cls = cfg.get(self.config_section, 'driver_class')
        uri = '{0:s}:{1:s}'.format(cfg.get(self.config_section, 'uri'), cfg.get(self.config_section, 'port'))
        user = cfg.get(self.config_section, 'username')
        pwd = cfg.get(self.config_section, 'password')

        # create JDBC connection
        self._conn = jaydebeapi.connect(jclassname=dr_cls, driver_args=[uri, user, pwd], jars=[dr_jar])

    def create_cursor(self):
        """Create a JDBC cursor instance"""

        return self.connection.cursor()

    def close(self):
        """Close JDBC connection"""

        if not self._conn:
            raise RuntimeError('Not connected to any database.')
        self.logger.debug('Closing JDBC connection.')
        self._conn.close()

