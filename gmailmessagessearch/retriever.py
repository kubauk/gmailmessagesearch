import base64
import email
import os
from datetime import timedelta
from threading import Event

import httplib2
import oauth2client
import pytz
from googleapiclient import discovery
from googleapiclient.http import BatchHttpRequest
from oauth2client import client
from oauth2client import tools
from oauth2client import file

_SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
_CLIENT_SECRET_FILE = 'client_secret.json'
_US_PACIFIC_TZ = pytz.timezone('US/Pacific')


def as_us_pacific(date):
    return date.astimezone(_US_PACIFIC_TZ)


def day_after(date):
    return date + timedelta(days=1)


def as_query_date(date):
    return date.strftime("%Y/%m/%d")


def decode_message(result):
    try:
        message_bytes = base64.urlsafe_b64decode(result['raw'])
        return email.message_from_string(message_bytes.decode('ascii'))
    except UnicodeDecodeError:
        pass


def add_message_and_unlock_if_finished(expected, messages, response, lock):
    messages.append(decode_message(response))
    if len(messages) == expected:
        lock.set()
    pass


def _fake_arg_parser():
    class FakeArgParser(object):
        auth_host_name = "localhost"
        noauth_local_webserver = False
        auth_host_port = [8080, 8090]
        logging_level = 'ERROR'
    return FakeArgParser()


class Retriever(object):
    _current_service = None

    def __init__(self, application_name, email_address, search_query,
                 secrets_directory=os.path.dirname(os.path.realpath(__file__))):
        super().__init__()
        self._args = _fake_arg_parser()
        self._application_name = application_name
        self._email_address = email_address
        self._search_query = search_query
        self._secrets_directory = secrets_directory

    def get_messages_for_date(self, message_date):
        return self._retrieve_messages(self._list_messages_for_day(message_date))

    def get_messages_for_date_range(self, after_date, before_date):
        return self._retrieve_messages(
            self._list_messages_for_days(after_date, before_date))

    def _get_service(self):
        if self._current_service is None:
            self._current_service = self._build_service(self._get_credentials())
        return self._current_service

    def _get_credentials(self):
        store = oauth2client.file.Storage(os.path.join(self._secrets_directory, "credentials.json"))
        flow = client.flow_from_clientsecrets(os.path.join(self._secrets_directory, _CLIENT_SECRET_FILE), _SCOPES)
        flow.user_agent = self._application_name
        return tools.run_flow(flow, store, self._args)

    def _list_messages_for_day(self, date):
        return self._list_messages_for_days(date, day_after(date))

    def _list_messages_for_days(self, after, before):
        query = '%s after:%s before:%s' % (self._search_query,
                                           as_query_date(as_us_pacific(after)),
                                           as_query_date(as_us_pacific(before)))
        service = self._get_service()
        result = service.users().messages().list(userId=self._email_address, q=query).execute()
        message_ids = result.get('messages', [])
        message_batches = [message_ids]

        while 'nextPageToken' in result:
            result = service.users().messages().list(userId=self._email_address,
                                                     pageToken=result['nextPageToken'],
                                                     q=query).execute()
            message_batches.append(result.get('messages', []))

        return message_batches

    def _retrieve_messages(self, message_batches):
        messages = list()
        lock = Event()
        batch = BatchHttpRequest()
        service = self._get_service()
        for message_ids in message_batches:
            for message_id in message_ids:
                batch.add(service.users().messages().get(userId=self._email_address, id=message_id['id'], format='raw'),
                          lambda id, response, exception:
                          add_message_and_unlock_if_finished(
                              len(message_ids), messages, response, lock))
            batch.execute()
            lock.wait()
        return messages

    @staticmethod
    def _build_service(current_credentials):
        http = current_credentials.authorize(httplib2.Http())
        return discovery.build('gmail', 'v1', http=http)
