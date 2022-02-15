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
        return None

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

class Me(VtigerStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "me"

class Leads(VtigerStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "query?query=select%20*%20from%20Leads%3B"


class Calendar(VtigerStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "query?query=select%20*%20from%20Calendar%3B"

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
                Leads(host=host, authenticator=auth),
                Me(host=host, authenticator=auth)
                # Calendar(host=host, authenticator=auth)
            ]
