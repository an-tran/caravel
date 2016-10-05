from flask.ext.appbuilder import Model
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.orm import relationship

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
    server_name = Column(String(250), ForeignKey('rest_server.server_name'))
    server = relationship('RestServerModel', backref='rest_datasources', foreign_keys=[server_name])


    @renders('database_name')
    def datasource_link(self):
        url = "/caravel/explore/{obj.type}/{obj.id}/".format(obj=self)
        name = escape(self.table_name)
        return Markup('<a href="{url}">{name}</a>'.format(**locals()))


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

