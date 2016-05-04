
from datetime import datetime
import pytz


class Details(object):

    def __init__(self, *args, **kwargs):
        object.__init__(self, *args, **kwargs)
        self.is_encrypted = False


class AuthContext(object):

    def __init__(self, *args, **kwargs):
        object.__init__(self, *args, **kwargs)
        self.user_name = 'DefaultU'
        self.name = 'DefaultU'
        self.tenant_name = 'DefaultU'
        self.login_tenant_name = 'DefaultU'
        self.details = Details()

    def set_context(self, username, tenant_name, login_tenant_name):
        pass

    def decrypt(self, value):
        return value

    def encrypt(self, value):
        return value

    def peek_context(self):
        return (self.user_name, self.tenant_name, 'domain')

    def tenant_time_zone(self):
        return pytz.timezone("US/Pacific")


class Crypto(object):

    def __init__(self, *args, **kwargs):
        object.__init__(self, *args, **kwargs)
        self.user_name = 'DefaultU'

    def extract_index(self, value, localattrs):
        return localattrs

crypto_utils = Crypto()

sec_context = AuthContext()
