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
from thriftpy.rpc import make_client, client_context
from thriftpy.transport import TTransportException
from xylose.scielodocument import Article, Journal, Issue


# URLJOIN Python 3 and 2 import compatibilities
try:
    from urllib.parse import urljoin
except:
    from urlparse import urljoin


LIMIT = 1000
DEFAULT_FROM_DATE = '1996-01-01'

logger = logging.getLogger(__name__)

EVENTS_STRUCT = namedtuple('event', 'code collection event date')


class ArticleMetaExceptions(Exception):
    pass


class UnauthorizedAccess(ArticleMetaExceptions):
    pass


class ServerError(ArticleMetaExceptions):
    pass


def dates_pagination(from_date, until_date):
    from_date = datetime.strptime(from_date, '%Y-%m-%d')
    until_date = datetime.strptime(until_date, '%Y-%m-%d')

    for year in range(from_date.year, until_date.year+1):

        dtbg = '%d-01-01' % year
        dtnd = '%d-12-31' % year

        if year == from_date.year:
            yield (from_date.isoformat()[:10], dtnd)
            continue

        if year == until_date.year:
            yield (dtbg, until_date.isoformat()[:10])
            continue

        yield (dtbg, dtnd)


class RestfulClient(object):

    ARTICLEMETA_URL = 'http://articlemeta.scielo.org'
    JOURNAL_ENDPOINT = '/api/v1/journal'
    ARTICLE_ENDPOINT = '/api/v1/article'
    ARTICLES_ENDPOINT = '/api/v1/articles'
    ISSUE_ENDPOINT = '/api/v1/issue'
    ISSUES_ENDPOINT = '/api/v1/issues'
    COLLECTION_ENDPOINT = '/api/v1/collection'
    ATTEMPTS = 10

    def __init__(self, domain=None):

        if domain:
            self.ARTICLEMETA_URL = domain

    def _do_request(self, url, params=None, timeout=3, method='GET'):

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
                    logger.error('Unautorized Access for (%s)', url)
                    logger.exception(UnauthorizedAccess())
                break
            except requests.RequestException as e:
                logger.error('fail retrieving data from (%s) attempt(%d/%d)', url, attempt+1, self.ATTEMPTS)
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
            'issn': code,
            'collection': collection
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

    def issues(
        self, collection=None, issn=None, from_date=None,
        until_date=None
    ):

        params = {
            'limit': 100
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
                url = urljoin(self.ARTICLEMETA_URL, self.ISSUES_ENDPOINT)
                issues = self._do_request(url, params=params)
                if issues is None:
                    break

                issues = issues.get('objects', [])

                if len(issues) == 0:
                    break

                for issue in issues:
                    yield Issue(issue)

                params['offset'] += 100

    def issues_by_identifiers(self, collection=None, issn=None, from_date=None,
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

    def document(self, code, collection, fmt='xylose', body=False):

        url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT)

        params = {
            'collection': collection,
            'code': code,
            'format': fmt,
            'body': str(body).lower()
        }

        result = self._do_request(url, params, )

        if not result:
            return None

        if fmt == 'xylose':
            return Article(result)

        return result

    def documents(
        self, collection=None, issn=None, from_date=None,
        until_date=None, fmt='xylose', body=False
    ):

        params = {
            'limit': 100,
            'fmt': fmt,
            'body': str(body).lower()
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
                url = urljoin(self.ARTICLEMETA_URL, self.ARTICLES_ENDPOINT)
                articles = self._do_request(url, params=params, timeout=10)

                if articles is None:
                    break

                articles = articles.get('objects', [])

                if len(articles) == 0:
                    break

                for article in articles:
                    yield Article(article)

                params['offset'] += 100

    def documents_by_identifiers(
        self, collection=None, issn=None, from_date=None,
        until_date=None, fmt='xylose', body=False, only_identifiers=False
    ):

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
                        fmt=fmt,
                        body=body
                    )

                    if fmt == 'xylose' and document and document.data:
                        yield document
                        continue

                    if fmt != 'xylose' and document:
                        yield document

                params['offset'] += LIMIT

    def documents_history(self, collection=None, issn=None, from_date=None,
                          until_date=None, fmt='xylose', only_identifiers=False, body=False):

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
                        fmt=fmt,
                        body=body
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
    ATTEMPTS = 10
    ARTICLEMETA_THRIFT = thriftpy.load(
        os.path.join(os.path.dirname(__file__))+'/thrift/articlemeta.thrift')

    def __init__(self, domain=None, admintoken=None):
        """
        Cliente thrift para o Articlemeta.
        """

        self.domain = domain or 'articlemeta.scielo.org:11621'
        self._set_address()
        self._admintoken = admintoken

    def _set_address(self):

        address = self.domain.split(':')

        self._address = address[0]
        try:
            self._port = int(address[1])
        except ValueError:
            self._port = 11620

    @property
    def client(self):

        return make_client(
            self.ARTICLEMETA_THRIFT.ArticleMeta,
            self._address,
            self._port
        )

    def client_cntxt(self):
        return client_context(
            self.ARTICLEMETA_THRIFT.ArticleMeta,
            self._address,
            self._port
        )

    def dispatcher(self, *args, **kwargs):

        for attempt in range(self.ATTEMPTS):
            try:
                func = args[0]
                with self.client_cntxt() as cl:
                    response = getattr(cl, func)(*args[1:], **kwargs)
                return response
            except TTransportException as e:
                msg = 'Error requesting articlemeta: %s args: %s kwargs: %s message: %s' % (
                    str(func), str(args[1:]), str(kwargs), str(e)
                )
                logger.warning("Request Retry (%d,%d): %s", attempt+1, self.ATTEMPTS, msg)
                time.sleep(self.ATTEMPTS*2)
            except self.ARTICLEMETA_THRIFT.ServerError as e:
                msg = 'Error requesting articlemeta: %s args: %s kwargs: %s message: %s' % (
                    str(func), str(args[1:]), str(kwargs), str(e)
                )
                logger.warning("Request Retry (%d,%d): %s", attempt+1, self.ATTEMPTS, msg)
                time.sleep(self.ATTEMPTS*2)
            except self.ARTICLEMETA_THRIFT.Unauthorized as e:
                msg = 'Unautorized access to articlemeta: %s args: %s kwargs: %s message: %s' % (
                    str(func), str(args[1:]), str(kwargs), str(e)
                )
                raise UnauthorizedAccess(msg)
            except self.ARTICLEMETA_THRIFT.ValueError as e:
                msg = 'Error requesting articlemeta: %s args: %s kwargs: %s message: %s' % (
                    str(func), str(args[1:]), str(kwargs), str(e)
                )
                raise ValueError(msg)
            except Exception as e:
                msg = 'Error requesting articlemeta: %s args: %s kwargs: %s message: %s' % (
                    str(func), str(args[1:]), str(kwargs), str(e)
                )
                time.sleep(self.ATTEMPTS*2)


        raise ServerError(msg)

    def getInterfaceVersion(self):
        """
        This method retrieve the version of the thrift interface.

        data: legacy SciELO Documents JSON Type 3.
        """

        version = self.dispatcher(
            'getInterfaceVersion'
        )

        return version

    def add_journal(self, data):
        """
        This method include new journals to the ArticleMeta.

        data: legacy SciELO Documents JSON Type 3.
        """

        journal = self.dispatcher(
            'add_journal',
            data,
            self._admintoken
        )

        return json.loads(journal)

    def add_issue(self, data):
        """
        This method include new issues to the ArticleMeta.

        data: legacy SciELO Documents JSON Type 3.
        """

        issue = self.dispatcher(
            'add_issue',
            data,
            self._admintoken
        )

        return json.loads(issue)

    def add_document(self, data):
        """
        This method include new issues to the ArticleMeta.

        data: legacy SciELO Documents JSON Type 3.
        """

        document = self.dispatcher(
            'add_article',
            data,
            self._admintoken
        )

        return json.loads(document)

    def journal(self, code, collection=None):

        journal = self.dispatcher(
            'get_journal',
            code,
            collection
        )

        if not journal:
            logger.warning('Journal not found for: %s_%s', collection, code)
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
        logger.info('Journal loaded: %s_%s', collection, code)

        return xjournal

    def journals(self, collection=None, issn=None, only_identifiers=False, limit=LIMIT):
        offset = 0

        while True:

            identifiers = self.dispatcher(
                'get_journal_identifiers',
                collection=collection, issn=issn, limit=limit,
                offset=offset
            )

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

            offset += limit

    def journals_history(self, collection=None, event=None, code=None,
                         from_date=None, until_date=None,
                         only_identifiers=False, limit=LIMIT):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.dispatcher(
                    'journal_history_changes',
                    collection=collection, event=event, code=code,
                    from_date=from_date, until_date=until_date,
                    limit=limit, offset=offset
                )

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

                    offset += limit

    def exists_journal(self, code, collection):

        return self.dispatcher(
            'exists_journal',
            code,
            collection
        )

    def exists_issue(self, code, collection):
        return self.dispatcher(
            'exists_issue',
            code,
            collection
        )

    def exists_document(self, code, collection):
        return self.dispatcher(
            'exists_article',
            code,
            collection
        )

    def set_aid(self, code, collection, aid):

        self.dispatcher(
            'set_aid',
            code,
            collection,
            aid,
            self._admintoken
        )

    def set_doaj_id(self, code, collection, doaj_id):
        self.dispatcher(
            'set_doaj_id',
            code, collection,
            doaj_id,
            self._admintoken
        )

    def issue(self, code, collection, replace_journal_metadata=True):
        issue = self.dispatcher(
            'get_issue',
            code=code,
            collection=collection,
            replace_journal_metadata=True
        )

        if not issue:
            logger.warning('Issue not found for: %s_%s', collection, code)
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

    def issues_bulk(
        self, collection=None, issn=None, from_date=None,
        until_date=None, extra_filter=None, limit=LIMIT
    ):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                issues = self.dispatcher(
                    'get_issues',
                    collection=collection, issn=issn, from_date=from_date,
                    until_date=until_date, limit=limit, offset=offset,
                    extra_filter=extra_filter
                )

                if issues is None:
                    break

                issues = json.loads(issues).get('objects', [])

                if len(issues) == 0:
                    break

                for issue in issues:

                    yield Issue(issue)

                offset += limit

    def issues(
        self, collection=None, issn=None, from_date=None,
        until_date=None, extra_filter=None, only_identifiers=False, limit=LIMIT
    ):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.dispatcher(
                    'get_issue_identifiers',
                    collection=collection, issn=issn, from_date=from_date,
                    until_date=until_date, limit=limit, offset=offset,
                    extra_filter=extra_filter
                )

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

                offset += limit

    def issues_history(
        self, collection=None, event=None, code=None,
        from_date=None, until_date=None, only_identifiers=False, limit=LIMIT
    ):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.dispatcher(
                    'issue_history_changes',
                    collection=collection, event=event, code=code,
                    from_date=from_date, until_date=until_date,
                    limit=limit, offset=offset
                )

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

                offset += limit

    def document(self, code, collection, replace_journal_metadata=True, fmt='xylose', body=False):

        article = self.dispatcher(
            'get_article',
            code=code,
            collection=collection,
            replace_journal_metadata=True,
            fmt=fmt,
            body=body
        )

        if not article:
            logger.warning('Document not found for: %s_%s', collection, code)
            return None

        if fmt == 'xylose':
            jarticle = None
            try:
                jarticle = json.loads(article)
            except:
                msg = 'Fail to load JSON when retrienving document: %s_%s' % (collection, code)
                raise ValueError(msg)

            xarticle = Article(jarticle)
            logger.info('Document loaded: %s_%s', collection, code)

            return xarticle

        logger.info('Document loaded: %s_%s', collection, code)
        return article

    def documents_bulk(
        self, collection=None, issn=None, from_date=None,
        until_date=None, fmt='xylose', body=False, extra_filter=None, limit=LIMIT
    ):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                articles = self.dispatcher(
                    'get_articles',
                    collection=collection, issn=issn,
                    from_date=from_date, until_date=until_date,
                    limit=limit, offset=offset,
                    extra_filter=extra_filter
                )

                if articles is None:
                    break

                articles = json.loads(articles).get('objects', [])

                if len(articles) == 0:
                    break

                for article in articles:

                    yield Article(article)

                offset += limit

    def documents(self, collection=None, issn=None, from_date=None,
                  until_date=None, fmt='xylose', body=False, extra_filter=None,
                  only_identifiers=False, limit=LIMIT):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.dispatcher(
                    'get_article_identifiers',
                    collection=collection, issn=issn,
                    from_date=from_date, until_date=until_date,
                    limit=limit, offset=offset,
                    extra_filter=extra_filter
                )

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
                        fmt=fmt,
                        body=body
                    )

                    yield document

                offset += limit

    def documents_history(self, collection=None, event=None, code=None,
                          from_date=None, until_date=None, fmt='xylose',
                          only_identifiers=False, limit=LIMIT):

        fdate = from_date or DEFAULT_FROM_DATE
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.dispatcher(
                    'article_history_changes',
                    collection=collection, event=event, code=code,
                    from_date=from_date, until_date=until_date,
                    limit=limit, offset=offset
                )

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

                offset += limit

    def collection(self, code):
        """
        Retrieve the collection ids according to the given 3 letters acronym
        """
        result = None
        result = self.dispatcher(
            'get_collection',
            code=code
        )

        if not result:
            logger.warning('Collection not found for: %s', code)
            return None

        return result

    def collections(self, only_identifiers=False):
        identifiers = self.dispatcher(
            'get_collection_identifiers'
        )

        for identifier in identifiers:
            if only_identifiers is True:
                yield identifier
                continue

            yield self.collection(identifier.code)

    def delete_journal(self, code, collection):

        result = None
        result = self.dispatcher(
            'delete_journal',
            code,
            collection,
            self._admintoken
        )

        return json.loads(result)

    def delete_issue(self, code, collection):

        result = None
        result = self.dispatcher(
            'delete_issue',
            code,
            collection,
            self._admintoken
        )

        return json.loads(result)

    def delete_document(self, code, collection):

        result = None
        result = self.dispatcher(
            'delete_article',
            code,
            collection,
            self._admintoken
        )

        return json.loads(result)
