"""Helpful criterion for pypika builders"""
from pypika.terms import Criterion


class ExistsCriterion(Criterion):
    """https://github.com/kayak/pypika/issues/278"""
    def __init__(self, container, alias=None):
        super(ExistsCriterion, self).__init__(alias)
        self.container = container

    @property
    def tables_(self):
        if not hasattr(self.container, 'tables_'):
            return []
        return self.container.tables_

    @property
    def is_aggregate(self):
        return False

    def fields(self):
        return []

    def get_sql(self, **kwargs):
        if 'subquery' in kwargs:
            del kwargs['subquery']
        kwargs['with_namespace'] = True
        return "EXISTS ({container})".format(
            container=self._get_container_sql(**kwargs)
        )

    def _get_container_sql(self, **kwargs):
        kwargs['quote_char'] = '"'
        return ''.join((
            'SELECT',
            self.container._from_sql(**kwargs),
            ' ' + ' '.join(
                join.get_sql(**kwargs) for join in self.container._joins
            ) if self.container._joins else '',
            self.container._prewhere_sql(**kwargs) if self.container._prewheres else '',
            self.container._where_sql(**kwargs) if self.container._wheres else '',
            self.container._group_sql(**kwargs) if self.container._groupbys else '',
            self.container._having_sql(**kwargs) if self.container._havings else '',
            self.container._orderby_sql(**kwargs) if self.container._orderbys else '',
        ))


def exists(query, alias=None):
    """Helper function for creating an exists criterion"""
    return ExistsCriterion(query, alias=alias)
