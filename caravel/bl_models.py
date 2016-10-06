import json

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
from caravel.models import AuditMixinNullable, Queryable

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


class CassandraDataSource(Model, AuditMixinNullable, Queryable):
    """ORM object referencing Cassandra datasources (tables)"""
    type = "cassandra"
    baselink = "cassandradatasourcemodelview"
    __tablename__ = "cassandra_datasources"
    id = Column(Integer, primary_key=True)
    # TODO: remove datasource_name, use keyspace and table instead
    datasource_name = Column(String(255), unique=True)
    is_featured = Column(Boolean, default=False)
    is_hidden = Column(Boolean, default=False)
    description = Column(Text)
    user_id = Column(Integer, ForeignKey('ab_user.id'))
    owner = relationship('User', backref='cassandra_datasources', foreign_keys=[user_id])
    cluster_name = Column(String(250), ForeignKey('cassandra_cluster.cluster_name'))
    # TODO: timezone offset, cache timeout
    # cassandra
    cassandra_keyspace = Column(String(250), nullable=False)
    cassandra_table = Column(String(250), nullable=False)
    # spark cluster
    spark_uri = Column(String(250), nullable=False)


class RestServerModel(Model, AuditMixinNullable):
    """ORM object referencing the Rest Datasource Server"""
    __tablename__ = 'rest_server'
    id = Column(Integer, primary_key=True)
    server_name = Column(String(250), unique=True)
    server_url = Column(String(250), unique=True)

    def __repr__(self):
        return "{}".format(self.server_url)

    def get_client(self, datasource):
        return RestClient(self, datasource)


class RestClient:
    METADATA_API_URL = '/api/datasource/{type}/{db}/{table}/meta'

    def __init__(self, server, datasource):
        self.server_url = server.server_url
        self.source_type = datasource.database_type
        self.db = datasource.database_name
        self.table = datasource.table_name

    def get_metadata(self):
        url = "%s%s" % (self.server_url,
                        self.METADATA_API_URL.format(self.source_type,
                                                     self.db,
                                                     self.table))
        return requests.get(url).json()

    def get_data(self):
        return None


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

    @renders('database_name')
    def datasource_link(self):
        url = "/caravel/explore/{obj.type}/{obj.id}/".format(obj=self)
        name = escape(self.table_name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))

    @property
    def full_name(self):
        return ("[{obj.database_name}]."
                "[{obj.table_name}]").format(obj=self)

    def fetch_metadata(self):
        """ Fetch metadata from Rest datasource and save to db as RestColums"""
        session = get_session()
        client = self.server.get_client(self)
        meta = client.get_meta()
        cols = meta['columns']
        if not cols:
            return
        for col in cols:
            col_obj = (session.query(RestColum)
                           .filter_by(datasource_id=self.id, column_name=cols)
                           .first())
            datatype = cols[col]['type']
            if not col_obj:
                col_obj = RestColum(datasource_id=self.id, column_name=col)
                session.add(col_obj)
            if datatype == "STRING":
                col_obj.groupby = True
                col_obj.filterable = True
            col_obj.type = cols[col]['type']
            session.flush()
            col_obj.datasource = self
            # TODO generate metrics
            col_obj.generate_metrics()
            session.flush()

    def query(
            self, groupby, metrics,
            granulariy,
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
        print "Query data from server %s/%s" % (self.server.server_url, self.api_endpoint)

        return None


class RestColum(Model, AuditMixinNullable):
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
    desciption = Column(Text)

    def __repr__(self):
        return self.column_name

    @property
    def isnum(self):
        return self.type in ('LONG', 'DOUBLE', 'FLOAT', 'INT')

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
            )
            metric.datasource_id = self.datasource_id
            if not m:
                new_metrics.append(metric)
                session.add(metric)
                session.flush

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
