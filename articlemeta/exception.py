
class ArticleMetaExceptions(Exception):
    pass


class UnauthorizedAccess(ArticleMetaExceptions):
    pass


class ServerError(ArticleMetaExceptions):
    pass
