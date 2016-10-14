# coding: utf-8
import os
import thriftpy
import json
import logging
from datetime import datetime
from datetime import timedelta

from thriftpy.rpc import make_client
from xylose.scielodocument import Article, Journal

LIMIT = 100
TIME_DELTA = 30

logger = logging.getLogger(__name__)


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


def articlemeta(address, port):

    pass


class RestfulClient(object):

    def journals(self, collection=None, issn=None):
        pass


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

    def journals(self, collection=None, issn=None):
        offset = 0

        while True:
            identifiers = self.client.get_journal_identifiers(collection=collection, issn=issn, limit=LIMIT, offset=offset)

            if len(identifiers) == 0:
                raise StopIteration

            for identifier in identifiers:

                journal = self.client.get_journal(
                    code=identifier.code[0], collection=identifier.collection)

                jjournal = json.loads(journal)

                xjournal = Journal(jjournal)

                logger.info('Journal loaded: %s_%s' % ( identifier.collection, identifier.code))

                yield xjournal

            offset += LIMIT

    def exists_article(self, code, collection):
        try:
            return self.client.exists_article(
                code,
                collection
            )
        except:
            msg = 'Error checking if document exists: %s_%s' % (collection, code)
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
                    raise StopIteration

                for identifier in identifiers:

                    document = self.document(
                        code=identifier.code,
                        collection=identifier.collection,
                        replace_journal_metadata=True,
                        fmt=fmt
                    )

                    yield document

                offset += LIMIT

    def collections(self):

        return [i for i in self.client.get_collection_identifiers()]
