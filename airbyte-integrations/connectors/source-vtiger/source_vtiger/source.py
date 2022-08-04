#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
import base64
from requests.auth import AuthBase

class BasicApiTokenAuthenticator(TokenAuthenticator):
    """basic Authorization header"""
    def __init__(self, username: str, password: str):
        token = base64.b64encode(f"{username}:{password}".encode("utf-8"))
        super().__init__(token.decode("utf-8"), auth_method="Basic")

# Basic full refresh stream
class VtigerStream(HttpStream, ABC):
    url_base = ""
    current_page = 1
    # batch / page size. it is recommended to use 100 as this is the implicit maximum by the vtiger api
    # https://help.vtiger.com/article/147111249-Rest-API-Manual#articleHeader19
    batch_size = 100

    primary_key = "id"

    def __init__(self, host: str, authenticator: Union[AuthBase, HttpAuthenticator] = None):
        self.url_base = f"https://{host}/restapi/v1/vtiger/default/"
        super().__init__(authenticator=authenticator)


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """

        returned_record_count = len(response.json()['result'])
        if self.batch_size == returned_record_count:
            self.current_page = self.current_page + 1
            return {
                'limit': self.batch_size,
                'offset': (self.current_page * self.batch_size) - self.batch_size
            }
        self.current_page = 1
        return None


    def get_query_url_string(self, model: str, next_page_token: Any) -> str:
        if next_page_token is None:
            limit = self.batch_size
            offset = 0
        else:
            limit = next_page_token['limit']
            offset = next_page_token['offset']

        self.logger.info("Getting: {} / page {}".format(model,self.current_page))
        # self.logger.info("query?query=select%20*%20from%20{}%20limit%20{},{}%3B".format(model, str(offset), str(limit)))
        return "query?query=select%20*%20from%20{}%20limit%20{},{}%3B".format(model, str(offset), str(limit))


    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        return [response.json()]

# Basic incremental stream
class IncrementalVtigerStream(VtigerStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}



class Accounts(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Accounts', next_page_token)

class Calendar(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Calendar', next_page_token)

class Contacts(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Contacts', next_page_token)

class Currency(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Currency', next_page_token)

class DocumentFolders(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('DocumentFolders', next_page_token)

class Documents(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Documents', next_page_token)

class EmailCampaigns(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('EmailCampaigns', next_page_token)

class Invoice(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Invoice', next_page_token)

class LineItem(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('LineItem', next_page_token)

class ModComments(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('ModComments', next_page_token)

class Payments(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Payments', next_page_token)

class SalesOrder(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('SalesOrder', next_page_token)

class Services(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Services', next_page_token)

class Users(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('Users', next_page_token)

class VtcmAccounts(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmaccounts', next_page_token)

class VtcmChildren(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmchildren', next_page_token)

class VtcmEducation(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmeducation', next_page_token)

class VtcmFamilies(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmfamilies', next_page_token)

class VtcmForms(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmforms', next_page_token)

class VtcmHealth(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmhealth', next_page_token)

class VtcmPrograms(VtigerStream):
    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.get_query_url_string('vtcmprograms', next_page_token)


# Source
class SourceVtiger(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        host = config['host']
        username = config['username']
        access_key = config['accessKey']
        
        #todo improve validation
        if not host:
            return False, "Input host is required"
        if not username:
            return False, "Input username is required"
        if not access_key:
            return False, "Input access key is required"
            
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BasicApiTokenAuthenticator(username=config['username'], password=config['accessKey'])
        host = config['host']
        return [
                Accounts(host=host, authenticator=auth),
                Calendar(host=host, authenticator=auth),
                Contacts(host=host, authenticator=auth),
                Currency(host=host, authenticator=auth),
                DocumentFolders(host=host, authenticator=auth),
                Documents(host=host, authenticator=auth),
                EmailCampaigns(host=host, authenticator=auth),
                Invoice(host=host, authenticator=auth),
                LineItem(host=host, authenticator=auth),
                ModComments(host=host, authenticator=auth),
                Payments(host=host, authenticator=auth),
                SalesOrder(host=host, authenticator=auth),
                Services(host=host, authenticator=auth),
                Users(host=host, authenticator=auth),
                VtcmAccounts(host=host, authenticator=auth),
                VtcmChildren(host=host, authenticator=auth),
                VtcmEducation(host=host, authenticator=auth),
                VtcmFamilies(host=host, authenticator=auth),
                VtcmForms(host=host, authenticator=auth),
                VtcmHealth(host=host, authenticator=auth),
                VtcmPrograms(host=host, authenticator=auth)
            ]
