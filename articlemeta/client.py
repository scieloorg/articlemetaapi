# coding: utf-8
import os
import thriftpy
import json
import logging
import time

from datetime import datetime
from datetime import timedelta

from collections import namedtuple

import requests
from thriftpy.rpc import make_client
from xylose.scielodocument import Article, Journal, Issue


# URLJOIN Python 3 and 2 import compatibilities
try:
    from urllib.parse import urljoin
except:
    from urlparse import urljoin


LIMIT = 1000
TIME_DELTA = 365
DEFAULT_FROM_DATE = '1900-01-01'

logger = logging.getLogger(__name__)

EVENTS_STRUCT = namedtuple('event', 'code collection event date')


class ArticleMetaExceptions(Exception):
    pass


class UnauthorizedAccess(ArticleMetaExceptions):
    pass


class ServerError(ArticleMetaExceptions):
    pass


def dates_pagination(from_date, until_date):
    td = timedelta(days=TIME_DELTA)
    td_plus = timedelta(days=TIME_DELTA+1)
    fdate = datetime.strptime(from_date, '%Y-%m-%d')
    udate = datetime.strptime(until_date, '%Y-%m-%d')

    while True:

        dtrange = fdate + td

        if dtrange > udate:
            yield (fdate.isoformat()[:10], udate.isoformat()[:10])
            return

        yield (fdate.isoformat()[:10], dtrange.isoformat()[:10])

        fdate += td_plus


class RestfulClient(object):

    ARTICLEMETA_URL = 'http://articlemeta.scielo.org'
    JOURNAL_ENDPOINT = '/api/v1/journal'
    ARTICLE_ENDPOINT = '/api/v1/article'
    ISSUE_ENDPOINT = '/api/v1/issue'
    COLLECTION_ENDPOINT = '/api/v1/collection'
    ATTEMPTS = 10

    def __init__(self, domain=None):

        if domain:
            self.ARTICLEMETA_URL = domain

    def _do_request(self, url, params=None, content=None, timeout=3, method='GET'):

        request = requests.get
        params = params if params else {}

        if method == 'POST':
            request = requests.post
        if method == 'DELETE':
            request = requests.delete

        result = None
        for attempt in range(self.ATTEMPTS):
            # Throttling requests to the API. Our servers will throttle accesses to the API from the same IP in 3 per second.
            # So, to not receive a "too many connections error (249 HTTP ERROR)", do not change this line.
            time.sleep(0.4)
            try:
                result = request(url, params=params, timeout=timeout)
                if result.status_code == 401:
                    logger.error('Unautorized Access for (%s)' % url)
                    logger.exception(UnauthorizedAccess())
                break
            except requests.RequestException as e:
                logger.error('fail retrieving data from (%s) attempt(%d/%d)' % (url, attempt+1, self.ATTEMPTS))
                logger.exception(e)
                continue

        if not result:
            return

        try:
            return result.json()
        except:
            return result.text

    def journal(self, code, collection):

        url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT)

        params = {
            'collection': collection,
            'issn': code
        }

        result = self._do_request(url, params)

        if not result:
            return None

        if len(result) != 1:
            return None

        xresult = Journal(result[0])

        return xresult

    def journals(self, collection=None, issn=None, only_identifiers=False):
        params = {
            'limit': LIMIT
        }

        if collection:
            params['collection'] = collection
        if issn:
            params['code'] = issn

        params['offset'] = 0
        while True:
            url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT + '/identifiers')
            identifiers = self._do_request(url, params=params).get('objects', [])

            if len(identifiers) == 0:
                raise StopIteration

            for identifier in identifiers:

                if only_identifiers is True:
                    yield identifier
                    continue

                journal = self.journal(
                    identifier['code'],
                    identifier['collection']
                )

                if journal and journal.data:
                    yield journal

            params['offset'] += LIMIT

    def journals_history(self, collection=None, event=None, code=None,
                         from_date=None, until_date=None,
                         only_identifiers=False):

        params = {
            'limit': LIMIT
        }

        if collection:
            params['collection'] = collection
        if code:
            params['code'] = code
        if event:
            params['event'] = event

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['offset'] = 0
            if from_date:
                params['from_date'] = from_date
            if until_date:
                params['until_date'] = until_date
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT + '/history')
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield (EVENTS_STRUCT(**identifier), None)
                        continue

                    if identifier['event'] == 'delete':
                        yield (EVENTS_STRUCT(**identifier), None)
                        continue

                    journal = self.journal(
                        identifier['code'],
                        identifier['collection']
                    )

                    if journal and journal.data:
                        yield (EVENTS_STRUCT(**identifier), journal)

                params['offset'] += LIMIT

    def exists_journal(self, code, collection):
        url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT + '/exists')

        params = {
            'collection': collection,
            'code': code
        }

        result = self._do_request(url, params=params).json()

        if result is True:
            return True

        return False

    def exists_issue(self, code, collection):
        url = urljoin(self.ARTICLEMETA_URL, self.ISSUE_ENDPOINT + '/exists')

        params = {
            'collection': collection,
            'code': code
        }

        result = self._do_request(url, params=params).json()

        if result is True:
            return True

        return False

    def exists_article(self, code, collection):
        url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT + '/exists')

        params = {
            'collection': collection,
            'code': code
        }

        result = self._do_request(url, params=params).json()

        if result is True:
            return True

        return False

    def issue(self, code, collection):

        url = urljoin(self.ARTICLEMETA_URL, self.ISSUE_ENDPOINT)

        params = {
            'collection': collection,
            'code': code
        }

        result = self._do_request(url, params)

        if not result:
            return None

        xresult = Issue(result)

        return xresult

    def issues(self, collection=None, issn=None, from_date=None,
               until_date=None, only_identifiers=False):

        params = {
            'limit': LIMIT
        }

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['from'] = from_date
            params['until'] = until_date
            params['offset'] = 0

            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ISSUE_ENDPOINT + '/identifiers')
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield identifier
                        continue

                    issue = self.issue(
                        identifier['code'],
                        identifier['collection']
                    )

                    if issue and issue.data:
                        yield issue

                params['offset'] += LIMIT

    def issues_history(self, collection=None, issn=None, from_date=None,
               until_date=None, only_identifiers=False):

        params = {
            'limit': LIMIT
        }

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['from'] = from_date
            params['until'] = until_date
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ISSUE_ENDPOINT + '/history')
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield (EVENTS_STRUCT(**identifier), None)
                        continue

                    if identifier['event'] == 'delete':
                        yield (EVENTS_STRUCT(**identifier), None)
                        continue

                    issue = self.issue(
                        identifier['code'],
                        identifier['collection']
                    )

                    if issue and issue.data:
                        yield (EVENTS_STRUCT(**identifier), issue)

                params['offset'] += LIMIT

    def document(self, code, collection, fmt='xylose'):

        url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT)

        params = {
            'collection': collection,
            'code': code,
            'format': fmt
        }

        result = self._do_request(url, params, )

        if not result:
            return None

        if fmt == 'xylose':
            return Article(result)

        return result

    def documents(self, collection=None, issn=None, from_date=None,
                  until_date=None, fmt='xylose', only_identifiers=False):

        params = {
            'limit': LIMIT
        }

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['from'] = from_date
            params['until'] = until_date
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT + '/identifiers')
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:
                    if only_identifiers is True:
                        yield identifier
                        continue

                    document = self.document(
                        identifier['code'],
                        identifier['collection'],
                        fmt=fmt
                    )
                    yield document

                    if fmt == 'xylose' and document and document.data:
                        yield document
                        continue

                    if fmt != 'xylose' and document:
                        yield document

                params['offset'] += LIMIT

    def documents_history(self, collection=None, issn=None, from_date=None,
                          until_date=None, fmt='xylose', only_identifiers=False):

        params = {
            'limit': LIMIT
        }

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['from'] = from_date
            params['until'] = until_date
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT + '/history')
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield (EVENTS_STRUCT(**identifier), None)
                        continue

                    if identifier['event'] == 'delete':
                        yield (EVENTS_STRUCT(**identifier), None)
                        continue

                    document = self.document(
                        identifier['code'],
                        identifier['collection'],
                        fmt=fmt
                    )

                    if fmt == 'xylose' and document and document.data:
                        yield (EVENTS_STRUCT(**identifier), document)
                        continue

                    if fmt != 'xylose' and document:
                        yield (EVENTS_STRUCT(**identifier), document)

                params['offset'] += LIMIT

    def collection(self, code):
        """
        Retrieve the collection ids according to the given 3 letters acronym
        """
        url = urljoin(self.ARTICLEMETA_URL, self.COLLECTION_ENDPOINT)

        params = {'code': code}

        result = self._do_request(url, params=params)

        if not result:
            return None

        return result

    def collections(self):

        url = urljoin(self.ARTICLEMETA_URL, self.COLLECTION_ENDPOINT + '/identifiers')

        result = self._do_request(url)

        if not result:
            return []

        return result


class ThriftClient(object):
    ARTICLEMETA_THRIFT = thriftpy.load(
        os.path.join(os.path.dirname(__file__))+'/thrift/articlemeta.thrift')

    def __init__(self, domain=None, admintoken=None):
        """
        Cliente thrift para o Articlemeta.
        """
        self.domain = domain or '127.0.0.1:11620'
        self._set_address()
        self._admintoken = admintoken

    def _set_address(self):

        address = self.domain.split(':')

        self._address = address[0]
        try:
            self._port = address[1]
        except:
            self._port = '11620'

    @property
    def client(self):

        client = make_client(
            self.ARTICLEMETA_THRIFT.ArticleMeta,
            self._address,
            self._port
        )
        return client

    def add_journal(self, data):
        """
        This method include new journals to the ArticleMeta.

        data: legacy SciELO Documents JSON Type 3.
        """

        try:
            journal = self.client.add_journal(data, self._admintoken)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            raise ServerError(e.message)
        except self.ARTICLEMETA_THRIFT.ValueError as e:
            raise ValueError(e.message)

        return json.loads(journal)

    def add_issue(self, data):
        """
        This method include new issues to the ArticleMeta.

        data: legacy SciELO Documents JSON Type 3.
        """

        try:
            issue = self.client.add_issue(data)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            raise ServerError(e.message)
        except self.ARTICLEMETA_THRIFT.ValueError as e:
            raise ValueError(e.message)

        return json.loads(issue)

    def add_document(self, data):
        """
        This method include new issues to the ArticleMeta.

        data: legacy SciELO Documents JSON Type 3.
        """

        try:
            document = self.client.add_article(data)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            raise ServerError(e.message)
        except self.ARTICLEMETA_THRIFT.ValueError as e:
            raise ValueError(e.message)

        return json.loads(document)

    def journal(self, code, collection):

        try:
            journal = self.client.get_journal(
                code=code,
                collection=collection
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error retrieving journal: %s_%s' % (collection, code)
            raise ServerError(msg)

        if not journal:
            logger.warning('Journal not found for: %s_%s' % (collection, code))
            return None

        jjournal = None

        try:
            jjournal = json.loads(journal)
        except:
            msg = 'Fail to load JSON when retrienving journal: %s_%s' % (
                collection, code
            )
            raise ValueError(msg)

        xjournal = Journal(jjournal)
        logger.info('Journal loaded: %s_%s' % (collection, code))

        return xjournal

    def journals(self, collection=None, issn=None, only_identifiers=False):
        offset = 0

        while True:
            try:
                identifiers = self.client.get_journal_identifiers(
                    collection=collection, issn=issn, limit=LIMIT, offset=offset
                )
            except self.ARTICLEMETA_THRIFT.ServerError as e:
                msg = 'Error retrieving list of journal identifiers: %s_%s' % (collection, issn)
                raise ServerError(msg)

            if len(identifiers) == 0:
                raise StopIteration

            for identifier in identifiers:

                identifier.code = identifier.code
                if only_identifiers is True:
                    yield identifier
                    continue

                journal = self.journal(
                    identifier.code,
                    identifier.collection
                )

                if journal and journal.data:
                    yield journal

            offset += LIMIT

    def journals_history(self, collection=None, event=None, code=None,
                         from_date=None, until_date=None, only_identifiers=False):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                try:
                    identifiers = self.client.journal_history_changes(
                        collection=collection, event=event, code=code,
                        from_date=from_date, until_date=until_date, limit=LIMIT,
                        offset=offset
                    )
                except self.ARTICLEMETA_THRIFT.ServerError as e:
                    msg = 'Error retrieving list of journal history: %s_%s' % (collection, code)
                    raise ServerError(msg)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:
                    identifier.code = identifier.code
                    if only_identifiers is True:
                        yield (identifier, None)
                        continue

                    if identifier.event == 'delete':
                        yield (identifier, None)
                        continue

                    journal = self.journal(
                        identifier.code,
                        identifier.collection
                    )

                    if journal and journal.data:
                        yield (identifier, journal)

                    offset += LIMIT

    def exists_journal(self, code, collection):
        try:
            return self.client.exists_journal(
                code,
                collection
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error checking if journal exists: %s_%s' % (collection, code)
            raise ServerError(msg)

    def exists_issue(self, code, collection):
        try:
            return self.client.exists_issue(
                code,
                collection
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error checking if issue exists: %s_%s' % (collection, code)
            raise ServerError(msg)

    def exists_document(self, code, collection):
        try:
            return self.client.exists_article(
                code,
                collection
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error checking if document exists: %s_%s' % (collection, code)
            raise ServerError(msg)

    def set_aid(self, code, collection, aid):
        try:
            article = self.client.set_aid(
                code,
                collection,
                aid
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error senting aid for document: %s_%s' % (collection, code)
            raise ServerError(msg)

    def set_doaj_id(self, code, collection, doaj_id):
        try:
            article = self.client.set_doaj_id(
                code,
                collection,
                doaj_id
            )
        except:
            msg = 'Error senting doaj id for document: %s_%s' % (collection, code)
            raise ServerError(msg)

    def issue(self, code, collection, replace_journal_metadata=True):
        try:
            issue = self.client.get_issue(
                code=code,
                collection=collection,
                replace_journal_metadata=True
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error retrieving issue: %s_%s' % (collection, code)
            raise ServerError(msg)

        if not issue:
            logger.warning('Issue not found for: %s_%s' % (collection, code))
            return None

        jissue = None
        try:
            jissue = json.loads(issue)
        except:
            msg = 'Fail to load JSON when retrienving document: %s_%s' % (collection, code)
            raise ValueError(msg)

        xissue = Issue(jissue)
        logger.info('Issue loaded: %s_%s' % (collection, code))

        return xissue

    def issues(self, collection=None, issn=None, from_date=None,
               until_date=None, extra_filter=None, only_identifiers=False):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                try:
                    identifiers = self.client.get_issue_identifiers(
                        collection=collection, issn=issn, from_date=from_date,
                        until_date=until_date, limit=LIMIT, offset=offset,
                        extra_filter=extra_filter)
                except self.ARTICLEMETA_THRIFT.ServerError as e:
                    msg = 'Error retrieving list of issue identifiers: %s_%s' % (collection, issn)
                    raise ServerError(msg)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield identifier
                        continue

                    issue = self.issue(
                        identifier.code,
                        identifier.collection,
                        replace_journal_metadata=True
                    )

                    if issue and issue.data:
                        yield (identifier, issue)

                offset += LIMIT

    def issues_history(self, collection=None, event=None, code=None,
                       from_date=None, until_date=None, only_identifiers=False):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                try:
                    identifiers = self.client.issue_history_changes(
                        collection=collection, event=event, code=code,
                        from_date=from_date, until_date=until_date, limit=LIMIT,
                        offset=offset
                    )
                except self.ARTICLEMETA_THRIFT.ServerError as e:
                    msg = 'Error retrieving list of issue history: %s_%s' % (collection, code)
                    raise ServerError(msg)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield (identifier, None)
                        continue

                    if identifier.event == 'delete':
                        yield (identifier, None)
                        continue

                    issue = self.issue(
                        identifier.code,
                        identifier.collection,
                        replace_journal_metadata=True
                    )

                    if issue and issue.data:
                        yield (identifier, issue)

                offset += LIMIT

    def document(self, code, collection, replace_journal_metadata=True, fmt='xylose'):
        try:
            article = self.client.get_article(
                code=code,
                collection=collection,
                replace_journal_metadata=True,
                fmt=fmt
            )
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error retrieving document: %s_%s' % (collection, code)
            raise ServerError(msg)

        if not article:
            logger.warning('Document not found for: %s_%s' % (collection, code))
            return None

        if fmt == 'xylose':
            jarticle = None
            try:
                jarticle = json.loads(article)
            except:
                msg = 'Fail to load JSON when retrienving document: %s_%s' % (collection, code)
                raise ValueError(msg)

            xarticle = Article(jarticle)
            logger.info('Document loaded: %s_%s' % (collection, code))

            return xarticle

        logger.info('Document loaded: %s_%s' % (collection, code))
        return article

    def documents(self, collection=None, issn=None, from_date=None,
                  until_date=None, fmt='xylose', extra_filter=None,
                  only_identifiers=False):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                try:
                    identifiers = self.client.get_article_identifiers(
                        collection=collection, issn=issn, from_date=from_date,
                        until_date=until_date, limit=LIMIT, offset=offset,
                        extra_filter=extra_filter)
                except self.ARTICLEMETA_THRIFT.ServerError as e:
                    msg = 'Error retrieving list of article identifiers: %s_%s' % (collection, issn)
                    raise ServerError(msg)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield identifier
                        continue

                    document = self.document(
                        identifier.code,
                        identifier.collection,
                        replace_journal_metadata=True,
                        fmt=fmt
                    )

                    yield document

                offset += LIMIT

    def documents_history(self, collection=None, event=None, code=None,
                          from_date=None, until_date=None, fmt='xylose',
                          only_identifiers=False):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                try:
                    identifiers = self.client.article_history_changes(
                        collection=collection, event=event, code=code,
                        from_date=from_date, until_date=until_date, limit=LIMIT,
                        offset=offset
                    )
                except self.ARTICLEMETA_THRIFT.ServerError as e:
                    msg = 'Error retrieving list of article history: %s_%s' % (collection, code)
                    raise ServerError(msg)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if only_identifiers is True:
                        yield (identifier, None)
                        continue

                    if identifier.event == 'delete':
                        yield (identifier, None)
                        continue

                    document = self.document(
                        identifier.code,
                        identifier.collection,
                        replace_journal_metadata=True,
                        fmt=fmt
                    )

                    yield (identifier, document)

                offset += LIMIT

    def collection(self, code):
        """
        Retrieve the collection ids according to the given 3 letters acronym
        """
        result = None
        try:
            result = self.client.get_collection(code=code)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error retrieving collection: %s_%s' % (code)
            raise ServerError(msg)

        if not result:
            logger.warning('Collection not found for: %s' % (code))
            return None

        return result

    def collections(self, only_identifiers=False):
        try:
            identifiers = self.client.get_collection_identifiers()
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error retrieving collections'
            raise ServerError(msg)

        for identifier in identifiers:
            if only_identifiers is True:
                yield identifier
                continue

            yield self.collection(identifier.code)

    def delete_journal(self, code, collection):

        result = None
        try:
            result = self.client.delete_journal(code, collection, self._admintoken)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error removing journal: %s_%s' % (collection, code)
            raise ServerError(msg)
        except self.ARTICLEMETA_THRIFT.Unauthorized as e:
            msg = 'Unautorized access trying remove journal: %s_%s' % (collection, code)
            raise UnauthorizedAccess(msg)

        return json.loads(result)

    def delete_issue(self, code, collection):

        result = None
        try:
            result = self.client.delete_issue(code, collection)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error removing issue: %s_%s' % (collection, code)
            raise ServerError(msg)
        except self.ARTICLEMETA_THRIFT.Unauthorized as e:
            msg = 'Unautorized access trying remove issue: %s_%s' % (collection, code)
            raise UnauthorizedAccess(msg)

        return json.loads(result)

    def delete_document(self, code, collection):

        result = None
        try:
            result = self.client.delete_article(code, collection)
        except self.ARTICLEMETA_THRIFT.ServerError as e:
            msg = 'Error removing document: %s_%s' % (collection, code)
            raise ServerError(msg)
        except self.ARTICLEMETA_THRIFT.Unauthorized as e:
            msg = 'Unautorized access trying remove document: %s_%s' % (collection, code)
            raise UnauthorizedAccess(msg)


        return json.loads(result)
