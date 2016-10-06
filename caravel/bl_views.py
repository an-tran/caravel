from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import logging
import re
import sys
import time
import traceback
from datetime import datetime, timedelta

import functools
import sqlalchemy as sqla

from flask import (
    g, request, redirect, flash, Response, render_template, Markup)
from flask_appbuilder import ModelView, CompactCRUDMixin, BaseView, expose
from flask_appbuilder.actions import action
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.decorators import has_access, has_access_api
from flask_appbuilder.widgets import ListWidget
from flask_babel import gettext as __
from flask_babel import lazy_gettext as _
from flask_appbuilder.models.sqla.filters import BaseFilter

import caravel
from caravel import (
    appbuilder, cache, db, models, viz, utils, app,
    sm, ascii_art, sql_lab, views
)

from caravel.source_registry import SourceRegistry
from caravel.models import DatasourceAccessRequest as DAR
from caravel.views import CaravelModelView, DeleteMixin, ListWidgetWithCheckboxes

from caravel.bl_models import RestServerModel, RestDatasourceModel, CassandraCluster

config = app.config
log_this = models.Log.log_this
can_access = utils.can_access
QueryStatus = models.QueryStatus



class CassandraClusterModelView(CaravelModelView, DeleteMixin):
    datamodel = SQLAInterface(CassandraCluster)
    add_columns = [
        'cluster_name', 'contact_hosts', 'contact_port'
    ]
    edit_columns = add_columns
    list_columns = ['cluster_name', 'contact_hosts']


appbuilder.add_separator("Sources")
if config['CASSANDRA_IS_ACTIVE']:
    appbuilder.add_view(CassandraClusterModelView,
                        name="Cassandra Clusters",
                        label="Cassandra Clusters",
                        icon="fa-cubes",
                        category="Sources",
                        category_label=__("Sources"),
                        category_icon='fa-database')


class RestServerModelView(CaravelModelView, DeleteMixin):
    datamodel = SQLAInterface(RestServerModel)
    add_columns = ['server_name', 'server_url']
    edit_columns = add_columns
    list_columns = add_columns


appbuilder.add_separator("Sources")
if config['REST_SERVER_IS_ACTIVE']:
    appbuilder.add_view(RestServerModelView,
                        name="Datasource Rest Server",
                        label="Datasource Rest Server",
                        icon="fa-cubes",
                        category="Sources",
                        category_label=__("Sources"),
                        category_icon='fa-database')

class RestDatasourceModelView(CaravelModelView, DeleteMixin):
    datamodel = SQLAInterface(RestDatasourceModel)
    list_widget = ListWidgetWithCheckboxes
    list_columns = ['datasource_link',  'server', 'database_type',
                    "database_name", 'table_name', 'changed_on_']
    order_columns = ['datasource_link', 'server', 'database_type', 'changed_on_']
    base_order = ['database_type', 'asc']
    add_columns = ['server', 'database_type', 'database_name',
                    'table_name', 'api_endpoint', 'description', 'owner',
                    'is_featured', 'is_hidden', 'offset',
                    'cache_timeout']
    edit_columns = add_columns
    description_columns = {
        'description': Markup(
            "Supports <a href='"
            "https://daringfireball.net/projects/markdown/'>markdown</a>"),
    }
    # base_filters = [['id', FilterDruidDatasource, lambda: []]]
    label_columns = {
        'datasource_link': _("Table"),
        'server': _("Server"),
        'description': _("Description"),
        'owner': _("Owner"),
        'is_featured': _("Is Featured"),
        'is_hidden': _("Is Hidden"),
        'api_endpoint': _("API Endpoint"),
        'offset': _("Time Offset"),
        'cache_timeout': _("Cache Timeout"),
        'database_type': _("Database Type"),
        'database_name': _("Database Name"),
        'table_type': _("Table Name"),
    }

    # TODO: post_add, post_update



if config['REST_SERVER_IS_ACTIVE']:
    appbuilder.add_view(
        RestDatasourceModelView,
        "Rest Datasources",
        label=__("Rest Datasources"),
        category="Sources",
        category_label=__("Sources"),
        icon="fa-cube")

if config['REST_SERVER_IS_ACTIVE']:
    appbuilder.add_link(
        "Refresh Rest Datasource Metadata",
        href='/caravel/refresh_rest_datasources/',
        category='Sources',
        category_label=__("Sources"),
        category_icon='fa-database',
        icon="fa-cog")
