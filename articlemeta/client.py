# coding: utf-8
import os
import thriftpy
import json
import logging
from datetime import datetime
from datetime import timedelta
from urllib.parse import urljoin
from collections import namedtuple

import requests
from thriftpy.rpc import make_client
from xylose.scielodocument import Article, Journal, Issue

LIMIT = 1000
TIME_DELTA = 365

logger = logging.getLogger(__name__)

EVENTS_STRUCT = namedtuple('event', 'code collection event date')


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
    ATTEMPTS = 10

    def __init__(self, admin_key=None):
        self.admin_key = admin_key

    def _do_request(self, url, params, content=None, timeout=3, method='GET'):

        request = requests.get
        params = params if params else {}
        params['admin'] = self.admin_key

        if method == 'POST':
            request = requests.post

        result = None
        for attempt in range(self.ATTEMPTS):
            try:
                result = request(url, params=params, timeout=timeout)
                break
            except Exception as e:
                logger.error('fail retrieving data from (%s) attempt(%d/%d)' % (url, attempt+1, self.ATTEMPTS))
                logger.exception(e)
                continue

        try:
            return result.json()
        except:
            return result.text

    def journal(self, collection, issn):

        url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT)

        params = {
            'collection': collection,
            'issn': issn
        }

        result = self._do_request(url, params)

        if len(result) != 1:
            return None

        xresult = Journal(result[0])

        return xresult

    def journals(self, collection=None, issn=None):
        params = {}

        if collection:
            params['collection'] = collection
        if issn:
            params['code'] = issn

        while True:
            url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT + '/identifiers')
            params['offset'] = offset
            params['limit'] = LIMIT
            identifiers = self._do_request(url, params=params).get('objects', [])

            if len(identifiers) == 0:
                raise StopIteration

            for identifier in identifiers:
                journal = self.journal(
                    issn=identifier['code'],
                    collection=identifier['collection']
                )

                yield journal

            params['offset'] += LIMIT

    def journals_history(self, collection=None, event=None, code=None,
                         from_date=None, until_date=None):

        params = {}

        if collection:
            params['collection'] = collection
        if code:
            params['code'] = code
        if event:
            params['event'] = event

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['offset'] = 0
            if from_date:
                params['from_date'] = from_date
            if until_date:
                params['until_date'] = until_date

            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.JOURNAL_ENDPOINT + '/history')
                params['limit'] = LIMIT
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if identifier['event'] == 'delete':
                        yield (EVENTS_STRUCT(**identifier), identifier, None)

                    journal = self.journal(
                        issn=identifier['code'],
                        collection=identifier['collection']
                    )

                    if journal and journal.data:
                        yield (EVENTS_STRUCT(**identifier), journal)

                params['offset'] += LIMIT

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
               until_date=None):

        params = {}

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ISSUE_ENDPOINT + '/identifiers')
                params['limit'] = LIMIT
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:
                    issue = self.issue(
                        code=identifier['code'],
                        collection=identifier['collection']
                    )
                    yield issue

                params['offset'] += LIMIT

    def issues_history(self, collection=None, issn=None, from_date=None,
               until_date=None):

        params = {}

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ISSUE_ENDPOINT + '/history')
                params['limit'] = LIMIT
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if identifier['event'] == 'delete':
                        yield (EVENTS_STRUCT(**identifier), identifier, None)
                        continue

                    issue = self.issue(
                        issn=identifier['code'],
                        collection=identifier['collection']
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
                  until_date=None, fmt='xylose'):

        params = {}

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT + '/identifiers')
                params['limit'] = LIMIT
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:
                    document = self.document(
                        code=identifier['code'],
                        collection=identifier['collection'],
                        fmt=fmt
                    )
                    yield document

                params['offset'] += LIMIT

    def documents_history(self, collection=None, issn=None, from_date=None,
                          until_date=None, fmt='xylose'):

        params = {}

        if collection:
            params['collection'] = collection

        if issn:
            params['issn'] = issn

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            params['offset'] = 0
            while True:
                url = urljoin(self.ARTICLEMETA_URL, self.ARTICLE_ENDPOINT + '/history')
                params['limit'] = LIMIT
                identifiers = self._do_request(url, params=params).get('objects', [])

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if identifier['event'] == 'delete':
                        yield (EVENTS_STRUCT(**identifier), identifier, None)
                        continue

                    document = self.document(
                        code=identifier['code'],
                        collection=identifier['collection'],
                        fmt=fmt
                    )

                    if fmt == 'xylose' and document and document.data:
                        yield (EVENTS_STRUCT(**identifier), document)

                    if fmt != 'xylose' and document:
                        yield (EVENTS_STRUCT(**identifier), document)

                params['offset'] += LIMIT


class ThirftClient(object):

    ARTICLEMETA_THRIFT = thriftpy.load(
        os.path.join(os.path.dirname(__file__))+'/thrift/articlemeta.thrift')

    def __init__(self, address, port):
        """
        Cliente thrift para o Articlemeta.
        """
        self._address = address
        self._port = port

    @property
    def client(self):

        client = make_client(
            self.ARTICLEMETA_THRIFT.ArticleMeta,
            self._address,
            self._port
        )
        return client

    def journal(self, collection, issn):

        try:
            journal = self.client.get_journal(
                code=issn,
                collection=collection
            )
        except:
            msg = 'Error retrieving journal: %s_%s' % (collection, code)
            raise ServerError(msg)

        jjournal = None
        try:
            jjournal = json.loads(journal)
        except:
            msg = 'Fail to load JSON when retrienving journal: %s_%s' % (
                collection, issn
            )
            raise ServerError(msg)

        if not jjournal:
            logger.warning('Journal not found for : %s_%s' % (collection, issn))
            return None

        xjournal = Journal(jjournal)
        logger.info('Journal loaded: %s_%s' % (collection, issn))

        return xjournal

    def journals(self, collection=None, issn=None):
        offset = 0

        while True:
            identifiers = self.client.get_journal_identifiers(
                collection=collection, issn=issn, limit=LIMIT, offset=offset
            )

            if len(identifiers) == 0:
                raise StopIteration

            for identifier in identifiers:
                journal = self.journal(
                    issn=identifier.code[0],
                    collection=identifier.collection
                )

                yield journal

            offset += LIMIT

    def journals_history(self, collection=None, event=None, code=None,
                         from_date=None, until_date=None):

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.client.journal_history_changes(
                    collection=collection, event=event, code=code,
                    from_date=from_date, until_date=until_date, limit=LIMIT,
                    offset=offset
                )

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if identifier.event == 'delete':
                        yield (identifier.event, identifier, None)

                    journal = self.journal(
                        issn=identifier.code[0],
                        collection=identifier.collection
                    )

                    if journal and journal.data:
                        yield (identifier, journal)

                    offset += LIMIT

    def exists_issue(self, code, collection):
        try:
            return self.client.exists_issue(
                code,
                collection
            )
        except:
            msg = 'Error checking if issue exists: %s_%s' % (collection, code)
            raise ServerError(msg)

    def exists_article(self, code, collection):
        try:
            return self.client.exists_article(
                code,
                collection
            )
        except:
            msg = 'Error checking if document exists: %s_%s' % (collection, code)
            raise ServerError(msg)

    def set_aid(self, code, collection, aid):
        try:
            article = self.client.set_aid(
                code,
                collection,
                aid
            )
        except:
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
        except:
            msg = 'Error retrieving issue: %s_%s' % (collection, code)
            raise ServerError(msg)

        jissue = None
        try:
            jissue = json.loads(issue)
        except:
            msg = 'Fail to load JSON when retrienving document: %s_%s' % (collection, code)
            raise ServerError(msg)

        if not jissue:
            logger.warning('Issue not found for : %s_%s' % (collection, code))
            return None

        xissue = Issue(jissue)
        logger.info('Issue loaded: %s_%s' % (collection, code))

        return xissue

    def issues(self, collection=None, issn=None, from_date=None,
               until_date=None, extra_filter=None):

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.client.get_issue_identifiers(
                    collection=collection, issn=issn, from_date=from_date,
                    until_date=until_date, limit=LIMIT, offset=offset,
                    extra_filter=extra_filter)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    issue = self.issue(
                        code=identifier.code,
                        collection=identifier.collection,
                        replace_journal_metadata=True
                    )

                    yield issue

                offset += LIMIT

    def issues_history(self, collection=None, event=None, code=None,
                       from_date=None, until_date=None):

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.client.issue_history_changes(
                    collection=collection, event=event, code=code,
                    from_date=from_date, until_date=until_date, limit=LIMIT,
                    offset=offset
                )

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if identifier.event == 'delete':
                        yield (identifier, None)
                        continue

                    issue = self.issue(
                        code=identifier.code,
                        collection=identifier.collection,
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
        except:
            msg = 'Error retrieving document: %s_%s' % (collection, code)
            raise ServerError(msg)

        if fmt == 'xylose':
            jarticle = None
            try:
                jarticle = json.loads(article)
            except:
                msg = 'Fail to load JSON when retrienving document: %s_%s' % (collection, code)
                raise ServerError(msg)

            if not jarticle:
                logger.warning('Document not found for : %s_%s' % (collection, code))
                return None

            xarticle = Article(jarticle)
            logger.info('Document loaded: %s_%s' % (collection, code))

            return xarticle

        logger.info('Document loaded: %s_%s' % (collection, code))
        return article

    def documents(self, collection=None, issn=None, from_date=None,
                  until_date=None, fmt='xylose', extra_filter=None):

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]
        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.client.get_article_identifiers(
                    collection=collection, issn=issn, from_date=from_date,
                    until_date=until_date, limit=LIMIT, offset=offset,
                    extra_filter=extra_filter)

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    document = self.document(
                        code=identifier.code,
                        collection=identifier.collection,
                        replace_journal_metadata=True,
                        fmt=fmt
                    )

                    yield document

                offset += LIMIT

    def documents_history(self, collection=None, event=None, code=None,
                          from_date=None, until_date=None, fmt='xylose'):

        fdate = from_date or '1500-01-01'
        udate = until_date or datetime.today().isoformat()[:10]

        for from_date, until_date in dates_pagination(fdate, udate):
            offset = 0
            while True:
                identifiers = self.client.article_history_changes(
                    collection=collection, event=event, code=code,
                    from_date=from_date, until_date=until_date, limit=LIMIT,
                    offset=offset
                )

                if len(identifiers) == 0:
                    break

                for identifier in identifiers:

                    if identifier.event == 'delete':
                        yield (identifier, None)

                    document = self.document(
                        code=identifier.code,
                        collection=identifier.collection,
                        replace_journal_metadata=True,
                        fmt=fmt
                    )

                    if ifdocument and document.data:
                        yield (identifier, document)

                offset += LIMIT

    def collection(self, code):
        """
        Retrieve the collection ids according to the given 3 letters acronym
        """
        return self.client.get_collection(code=collection)

    def collections(self):

        return [i for i in self.client.get_collection_identifiers()]
