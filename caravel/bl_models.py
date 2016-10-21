import calendar
import json
import logging
from collections import namedtuple

from datetime import datetime
import pandas
import requests
from flask.ext.appbuilder import Model
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.orm import relationship

import caravel
from caravel import get_session
from caravel import utils
from caravel.models import AuditMixinNullable, Queryable, QueryResult

from flask import escape, g, Markup, request
from flask_appbuilder import Model
from flask_appbuilder.models.mixins import AuditMixin
from flask_appbuilder.models.decorators import renders
from flask_babel import lazy_gettext as _


class CassandraCluster(Model, AuditMixinNullable):
    """ORM object referencing the cassandra clusters"""

    __tablename__ = 'cassandra_cluster'
    id = Column(Integer, primary_key=True)
    cluster_name = Column(String(250), unique=True)
    contact_hosts = Column(String(250))
    contact_port = Column(Integer, default=9042)

    def __repr__(self):
        return self.cluster_name

    @property
    def perm(self):
        return "[{obj.cluster_name}].(id:{obj.id})".format(obj=self)


# class CassandraDataSource(Model, AuditMixinNullable, Queryable):
#     """ORM object referencing Cassandra datasources (tables)"""
#     type = "cassandra"
#     baselink = "cassandradatasourcemodelview"
#     __tablename__ = "cassandra_datasources"
#     id = Column(Integer, primary_key=True)
#     #: remove datasource_name, use keyspace and table instead
#     datasource_name = Column(String(255), unique=True)
#     is_featured = Column(Boolean, default=False)
#     is_hidden = Column(Boolean, default=False)
#     description = Column(Text)
#     user_id = Column(Integer, ForeignKey('ab_user.id'))
#     owner = relationship('User', backref='cassandra_datasources', foreign_keys=[user_id])
#     cluster_name = Column(String(250), ForeignKey('cassandra_cluster.cluster_name'))
#     #: timezone offset, cache timeout
#     # cassandra
#     cassandra_keyspace = Column(String(250), nullable=False)
#     cassandra_table = Column(String(250), nullable=False)
#     # spark cluster
#     spark_uri = Column(String(250), nullable=False)


class RestServerModel(Model, AuditMixinNullable):
    """ORM object referencing the Rest Datasource Server"""
    __tablename__ = 'rest_server'
    id = Column(Integer, primary_key=True)
    server_name = Column(String(250), unique=True)
    server_url = Column(String(250), unique=True)
    cache_timeout = Column(Integer)

    def __repr__(self):
        return "{}".format(self.server_url)

    def get_client(self, datasource):
        return RestClient(self, datasource)

    def refresh_datasources(self):
        session = get_session()
        datasources = (session.query(RestDatasourceModel)
                       .filter_by(server_url=self.server_url).all())
        for d in datasources:
            d.fetch_metadata()

    @property
    def perm(self):
        return "[{obj.server_url}].(id:{obj.id})".format(obj=self)


class RestClient(object):
    API_URL = '/api/datasources/{db_type}/{db}/{table}'

    def __init__(self, server, datasource):
        self.server_url = server.server_url.rstrip("/")
        self.db_type = datasource.database_type
        self.db = datasource.database_name
        self.table = datasource.table_name

    def get_access_token_temp(self):
        url = "http://%s/login" % (self.server_url)
        resp = requests.post(url, data={"username": "admin", "password": "admin"})
        if resp.ok:
            return resp.json()['jwtToken']


    def get_metadata(self):
        url = "http://%s%s/meta" % (self.server_url,
                        self.API_URL.format(**self.__dict__))
        access_token = self.get_access_token_temp()
        resp = requests.get(url, headers={"Authorization": access_token})
        if resp.ok:
            logging.debug(resp.json())
        else:
            logging.error(resp.status_code, resp.reason)
        return resp.json()

    def get_data(self):
        return None

    def getDataframe(self, reqparams):
        url = "http://%s%s" % (self.server_url,
                                    self.API_URL.format(**self.__dict__))
        resp = requests.get(url.rstrip("/"),
                            params=reqparams,
                            headers={"Authorization": self.get_access_token_temp()})
        # TODO: json to df
        print resp.reason
        result = resp.json()
        self.result = result['result']
        self.query_type = result['query_type']
        return self.export_pandas()

    def export_pandas(self):
        """
        Export the current query result to a Pandas DataFrame object.

        :return: The DataFrame representing the query result
        :rtype: DataFrame
        :raise NotImplementedError:

        Example

        .. code-block:: python
            :linenos:

                >>> top = client.topn(
                        datasource='twitterstream',
                        granularity='all',
                        intervals='2013-10-04/pt1h',
                        aggregations={"count": doublesum("count")},
                        dimension='user_name',
                        filter = Dimension('user_lang') == 'en',
                        metric='count',
                        threshold=2
                    )

                >>> df = top.export_pandas()
                >>> print df
                >>>    count                 timestamp      user_name
                    0      7  2013-10-04T00:00:00.000Z         user_1
                    1      6  2013-10-04T00:00:00.000Z         user_2
        """
        if self.result:
            if self.query_type == "timeseries":
                nres = [list(v['result'].items()) + [('timestamp', v['timestamp'])]
                        for v in self.result]
                nres = [dict(v) for v in nres]
            elif self.query_type == "topN":
                nres = []
                for item in self.result:
                    timestamp = item['timestamp']
                    results = item['result']
                    tres = [dict(list(res.items()) + [('timestamp', timestamp)])
                            for res in results]
                    nres += tres
            elif self.query_type == "groupby":
                nres = [list(v.items()) for v in self.result]
                nres = [dict(v) for v in nres]
            else:
                raise NotImplementedError('Pandas export not implemented for query type: {0}'.format(self.query_type))

            df = pandas.DataFrame(nres)
            return df


class RestDatasourceModel(Model, AuditMixinNullable, Queryable):
    """ORM object reference generic data source provided by rest server"""
    type = "rest"
    baselink = "RestServerModel"
    __tablename__ = "rest_datasources"
    id = Column(Integer, primary_key=True)
    api_endpoint = Column(String(250), nullable=False)
    database_type = Column(String(250), nullable=False)
    database_name = Column(String(250), nullable=False)
    table_name = Column(String(250), nullable=False)
    # other
    is_featured = Column(Boolean, default=False)
    is_hidden = Column(Boolean, default=False)
    description = Column(Text)
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    owner = relationship('User', backref='rest_datasources', foreign_keys=[user_id])
    offset = Column(Integer, default=0)
    cache_timeout = Column(Integer)
    # server
    server_url = Column(String(250), ForeignKey('rest_server.server_url'))
    server = relationship('RestServerModel', backref='rest_datasources', foreign_keys=[server_url])

    def __repr__(self):
        return "{}.{}.{}".format(self.database_type, self.database_name, self.table_name)

    @renders('database_name')
    def datasource_link(self):
        url = "/caravel/explore/{obj.type}/{obj.id}/".format(obj=self)
        name = escape(self.table_name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))

    @property
    def full_name(self):
        return ("[{obj.database_type}]."
                "[{obj.database_name}]."
                "[{obj.table_name}]").format(obj=self)

    @property
    def metrics_combo(self):
        return sorted([(m.metric_name, m.verbose_name) for m in self.metrics],
                      key=lambda x: x[1])

    @property
    def database(self):
        return self.server

    @property
    def num_cols(self):
        return [c.column_name for c in self.columns if c.isnum]

    @property
    def name(self):
        return self.table_name

    @property
    def default_endpoint(self):
        return ""

    @property
    def perm(self):
        return (
            "[{obj.server_url}].[{obj.full_name}]"
            "(id:{obj.id})").format(obj=self)

    @property
    def dttm_cols(self):
        return [c.column_name for c in self.columns if c.is_dttm]

    @property
    def any_dttm_col(self):
        cols = self.dttm_cols
        if cols:
            return cols[0]

    def grains(self):
        Grain = namedtuple('Grain', 'name label function')
        grains = (
            Grain('Time Column', _('Time Column'), '{col}'),
            Grain('hour', _('hour'), '{col}'),
            Grain('day', _('day'), '{col}'),
            Grain('month', _('month'), '{col}')
        )
        return grains

    def fetch_metadata(self):
        """ Fetch metadata from Rest datasource and save to db as RestColumns"""
        session = get_session()
        client = self.server.get_client(self)
        meta = client.get_metadata()
        cols = meta['columns']
        if not cols:
            return
        for col in cols:
            col_obj = (session.query(RestColumn)
                           .filter_by(datasource_id=self.id, column_name=col)
                           .first())
            datatype = cols[col]['type']
            if not col_obj:
                col_obj = RestColumn(datasource_id=self.id, column_name=col, type=cols[col]['type'])
                session.add(col_obj)
                col_obj.sum = col_obj.isnum
            if datatype == "STRING":
                col_obj.groupby = True
                col_obj.filterable = True
            if (cols[col].get('isDateTime', None)) or (datatype == "DATETIME") or (datatype == "TIMESTAMP"):
                col_obj.is_dttm = True

            session.flush()
            col_obj.datasource = self
            col_obj.generate_metrics()
            session.flush()

    def query(
            self, groupby, metrics,
            granularity,
            from_dttm, to_dttm,
            filter=None,
            is_timeseries=True,
            timeseries_limit=None,
            row_limit=None,
            inner_from_dttm=None, inner_to_dttm=None,
            orderby=None,
            extras=None,
            select=None,
            columns=None):
        """Call rest api to get data from server"""
        qry_start_dttm = datetime.now()
        logging.info("Query data from server %s/%s" % (self.server.server_url, self.api_endpoint))
        reqparams = {
            'granularity': granularity,
            'groupby': groupby,
            'metrics': metrics,
            'from_dttm': calendar.timegm(from_dttm.timetuple()) * 1000,
            'to_dttm': calendar.timegm(to_dttm.timetuple()) * 1000,
            'filter': filter,
            'is_timeseries': is_timeseries,
            'row_limit': row_limit,
            'orderby': orderby,
            'select': select,
            'columns': columns
        }
        reqparams.update(extras)
        restClient = RestClient(self.server, self)
        df = restClient.getDataframe(reqparams)
        cols = []
        if 'timestamp' in df.columns:
            cols += ['timestamp']
        cols += [col for col in groupby if col in df.columns]
        cols += [col for col in metrics if col in df.columns]
        df = df[cols]
        return QueryResult(
            df=df,
            query="",
            duration=datetime.now() - qry_start_dttm)


class RestColumn(Model, AuditMixinNullable):
    """ORM model for storing rest datasource column metadata"""

    __tablename__ = 'rest_colums'
    id = Column(Integer, primary_key=True)
    datasource_id = Column(Integer,
                           ForeignKey('rest_datasources.id'))
    datasource = relationship('RestDatasourceModel', backref='columns',
                              enable_typechecks=False)
    column_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    type = Column(String(32))
    groupby = Column(Boolean, default=False)
    count_distinct = Column(Boolean, default=False)
    sum = Column(Boolean, default=False)
    max = Column(Boolean, default=False)
    min = Column(Boolean, default=False)
    filterable = Column(Boolean, default=False)
    description = Column(Text)
    is_dttm = Column(Boolean, default=False)

    def __repr__(self):
        return self.column_name

    @property
    def isnum(self):
        return self.type in ('LONG', 'DOUBLE', 'FLOAT', 'INT', 'BIGINT', 'DECIMAL', 'VARINT', 'COUNTER')

    def generate_metrics(self):
        """Generate metric from column metadata and save to db"""
        M = RestMetric
        metrics = []
        metrics.append(M(
            metric_name='count',
            verbose_name='COUNT(*)',
            metric_type='count',
            json=json.dumps({'type': 'count', 'name': 'count'})
        ))

        if self.type in ('DOUBLE', 'FLOAT'):
            corrected_type = 'DOUBLE'
        else:
            corrected_type = self.type

        if self.sum and self.isnum:
            mt = corrected_type.lower() + 'Sum'
            name = 'sum__' + self.column_name
            metrics.append(M(
                metric_name=name,
                metric_type='sum',
                verbose_name='SUM({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.min and self.isnum:
            mt = corrected_type.lower() + 'Min'
            name = 'min__' + self.column_name
            metrics.append(M(
                metric_name=name,
                metric_type='min',
                verbose_name='MIN({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.max and self.isnum:
            mt = corrected_type.lower() + 'Max'
            name = 'max__' + self.column_name
            metrics.append(M(
                metric_name=name,
                metric_type='max',
                verbose_name='MAX({})'.format(self.column_name),
                json=json.dumps({
                    'type': mt, 'name': name, 'fieldName': self.column_name})
            ))
        if self.count_distinct:
            name = 'count_distinct__' + self.column_name
            mt = 'count_distinct'
            metrics.append(M(
                metric_name=name,
                verbose_nam='COUNT(DISTINCT {})'.format(self.column_name),
                metric_type='count_distinct',
                json=json.dump({
                    'type': 'cardinality', 'name': name, 'fieldNames': [self.column_name]
                })
            ))

        #save to db
        session = get_session()
        new_metrics = []
        for metric in metrics:
            m = (
                session.query(M)
                    .filter(M.metric_name == metric.metric_name)
                    .filter(M.datasource_id == self.datasource_id)
                    .filter(RestServerModel.server_url == self.datasource.server_url)
                    .first())
            metric.datasource_id = self.datasource_id
            if not m:
                new_metrics.append(metric)
                session.add(metric)
                session.flush()

        utils.init_metrics_perm(caravel, new_metrics)


class RestMetric(Model, AuditMixinNullable):
    """ORM object for referencing metric of Rest datasource"""

    __tablename__ = 'rest_metrics'
    id = Column(Integer, primary_key=True)
    metric_name = Column(String(512))
    verbose_name = Column(String(1024))
    metric_type = Column(String(32))
    datasource_id = Column(Integer,
                           ForeignKey('rest_datasources.id'))
    datasource = relationship('RestDatasourceModel', backref='metrics',
                              enable_typechecks=False)
    json = Column(Text)
    description = Column(Text)
    is_restricted = Column(Boolean, default=False, nullable=True)
    d3format = Column(String(128))

    @property
    def json_obj(self):
        try:
            obj = json.load(self.json)
        except Exception:
            obj = {}
        return obj

    @property
    def perm(self):
        return (
            "{parent_name}.[{obj.metric_name}](id:{obj.id})"
        ).format(obj=self,
                 parent_name=self.datasource.full_name
                 ) if self.datasource else None
