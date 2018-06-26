import requests
import os
import unittest

from requests_kerberos import HTTPKerberosAuth, REQUIRED


class KerberosFunctionalTestCase(unittest.TestCase):
    """
    This test is designed to run functional tests against a live website
    secured with Kerberos authentication. See .travis.sh for the script that
    is used to setup a Kerberos realm and Apache site.
        
    For this test to run the 2 environment variables need to be set
        KERBEROS_PRINCIPAL: The principal to authenticate with (user@REALM.COM)
            Before running this test you need to ensure you have gotten a valid
            ticket for the user in that realm using kinit.
        KERBEROS_URL: The URL (http://host.realm.com) to authenticate with
            This need to be set up before hand          
    """

    def setUp(self):
        """Setup."""
        self.principal = os.environ.get('KERBEROS_PRINCIPAL', None)
        self.url = os.environ.get('KERBEROS_URL', None)

        # Skip the test if not set
        if self.principal is None:
            raise unittest.SkipTest("KERBEROS_PRINCIPAL is not set, skipping functional tests")
        if self.url is None:
            raise unittest.SkipTest("KERBEROS_URL is not set, skipping functional tests")

    def test_successful_http_call(self):
        session = requests.Session()
        if self.url.startswith("https://"):
            session.verify = False

        session.auth = HTTPKerberosAuth(mutual_authentication=REQUIRED, principal=self.principal)
        request = requests.Request('GET', self.url)
        prepared_request = session.prepare_request(request)

        response = session.send(prepared_request)

        assert response.status_code == 200, "HTTP response with kerberos auth did not return a 200 error code"

if __name__ == '__main__':
    unittest.main()
