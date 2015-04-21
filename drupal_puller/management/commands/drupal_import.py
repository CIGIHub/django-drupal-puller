from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.timezone import utc, make_aware
from datetime import datetime
from collections import namedtuple
from optparse import make_option


import mysql.connector
import importlib
import pytz


class BaseImporter():
    taxonomy_term_data_table_name = 'term_data'
    load_url_aliases_query = "SELECT pid, src, dst FROM url_alias"

    def __init__(self, app):
        self.site_name = app
        self.connection = None

    @staticmethod
    def convert_drupal_time(original):
        try:
            new_date = datetime.strptime(original, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            try:
                new_date = datetime.strptime(original, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                new_date = datetime.strptime(original, "%Y-%m-%d")

        new_date = new_date.replace(tzinfo=utc)
        return new_date

    def open_connection(self):
        config = self.get_database_configuration()
        self.connection = mysql.connector.connect(**config)

    def close_connection(self):
        self.connection.close()

    def get_database_configuration(self):
        return settings.site_database_config[self.site_name]

    def load_terms(self, model_class, connection):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()
        query = "SELECT tid, name FROM {0} WHERE vid=%s".format(self.taxonomy_term_data_table_name)
        cursor.execute(query, (model_class.vocabulary_id(),))
        for (tid, name) in cursor:
            term, created = model_class.objects.get_or_create(source_id=tid)
            term.name = name
            term.save()

            if created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        print("%ss: Added %d, Update %d" % (model_class.__name__, added_count, updated_count))

    def load_url_aliases(self, connection, alias_model):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        query = self.load_url_aliases_query
        cursor.execute(query)
        for (pid, src, dst) in cursor:
            alias, created = alias_model.objects.get_or_create(pid=pid)
            alias.src = src
            alias.dst = dst
            alias.save()

            if created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        print("Url Aliases: Added %d, Update %d" % (added_count, updated_count))

    def load_drupal_nodes(self, connection, content_type, content_type_table, page_model, alias_model,
                          additional_field_list=None, additional_field_setter=None, page_matcher=None):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        extra_fields = ""
        if additional_field_list:
            extra_fields = ", ct1.%s" % ", ct1.".join(additional_field_list)

        query = "SELECT n.nid, n.vid, n.title, n.status, n.created, n.changed %s "\
                "FROM  %s ct1 "\
                "LEFT OUTER JOIN %s ct2 "\
                "ON (ct1.nid = ct2.nid AND ct1.vid < ct2.vid) "\
                "INNER JOIN node n "\
                "ON ct1.nid = n.nid and ct1.vid = n.vid "\
                "WHERE ct2.nid IS NULL "\
                "ORDER BY ct1.nid " % (extra_fields, content_type_table, content_type_table)

        cursor.execute(query)
        for values in cursor:
            nid = values[0]
            vid = values[1]
            title = values[2]
            status = values[3]
            created_ts = values[4]
            changed_ts = values[5]

            node, node_created = content_type.objects.get_or_create(nid=nid)
            node.vid = vid
            node.title = title
            node.status = status
            node.created = datetime.fromtimestamp(created_ts)
            node.changed = datetime.fromtimestamp(changed_ts)

            if additional_field_setter:
                extra_values = values[6:]
                additional_field_setter(node, extra_values)

            node.save()

            if page_matcher:
                page_matcher(node, page_model, alias_model)
            else:
                self.match_to_pages(node, page_model, alias_model)


            if node_created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        print("%s: Added %d, Update %d" % (content_type.__name__, added_count, updated_count))

    def load_node_references(self, connection, content_type, content_type_table,
                             linked_content_type, linked_content_type_table,
                             linked_content_field, linker):

        linked_nodes = 0
        unlinked_nodes = 0

        cursor = connection.cursor()

        query = "SELECT ct1.nid, ct1.vid, f.{linked_content_field}_nid " \
                "FROM {content_type_table} ct1 " \
                "LEFT OUTER JOIN {content_type_table} ct2 " \
                "ON (ct1.nid = ct2.nid AND ct1.vid < ct2.vid) " \
                "INNER JOIN content_{linked_content_field} f " \
                "ON ct1.nid = f.nid and ct1.vid = f.vid " \
                "WHERE ct2.nid IS NULL " \
                "AND f.{linked_content_field}_nid IS NOT NULL " \
                "ORDER BY ct1.nid " \
                .format(linked_content_field=linked_content_field,
                        content_type_table=content_type_table
                        )

        cursor.execute(query)
        for (nid, vid, linked_nid) in cursor:
            ct_object = content_type.objects.get(nid=nid)

            if linked_nid:
                try:
                    linked_node = linked_content_type.objects.get(nid=linked_nid)
                    linker(ct_object, linked_node)
                    ct_object.save()
                    linked_nodes += 1
                except linked_content_type.DoesNotExist:
                    unlinked_nodes += 1
                    print("Exception: Unlinked Node ID: %s" % linked_nid)
            else:
                unlinked_nodes += 1

        cursor.close()

        print("Linked Nodes: %s, Unlinked Nodes: %s" % (linked_nodes, unlinked_nodes))

    @staticmethod
    def load_linked_data_field(connection, content_type, content_type_table, linked_content_field, linker):
        cursor = connection.cursor()

        query = "SELECT ct1.nid, f.%s_value " \
                "FROM  %s ct1 " \
                "LEFT OUTER JOIN %s ct2 " \
                "ON (ct1.nid = ct2.nid AND ct1.vid < ct2.vid) " \
                "INNER JOIN content_%s f " \
                "ON ct1.nid = f.nid AND ct1.vid = f.vid " \
                "WHERE ct2.nid IS NULL AND f.%s_value IS NOT NULL " \
                "ORDER BY ct1.nid " % (linked_content_field, content_type_table, content_type_table,
                                       linked_content_field, linked_content_field)

        cursor.execute(query)
        for (nid, data_value) in cursor:
            ct_object = content_type.objects.get(nid=nid)

            linker(ct_object, data_value)
            ct_object.save()

        cursor.close()

        #print "Unlinked Authors Updated"

    @staticmethod
    def match_to_pages(node, page_model, alias_model):
        try:
            src = "node/%d" % node.nid

            page, created = page_model.objects.get_or_create(page_path="/%s" % src)
            node.pages.add(page)

            page, created = page_model.objects.get_or_create(page_path="/%s/" % src)
            node.pages.add(page)

            aliases = alias_model.objects.filter(src=src)
            for alias in aliases:
                node.aliases.add(alias)

                page_path = "/%s" % alias.dst
                page, created = page_model.objects.get_or_create(page_path=page_path)
                node.pages.add(page)

                page_path = "/%s/" % alias.dst
                page, created = page_model.objects.get_or_create(page_path=page_path)
                node.pages.add(page)

        except alias_model.DoesNotExist:
            print("Error could not find alias")

ColumnMap = namedtuple('ColumnMap', 'drupal_name model_name type_or_map')
def column_map(drupal_name, model_name=None, type_or_map=None):
    if model_name is None:
        return ColumnMap(drupal_name, drupal_name, type_or_map)
    else:
        return ColumnMap(drupal_name, model_name, type_or_map)


class Drupal7BaseImporter(BaseImporter):
    taxonomy_term_data_table_name = 'taxonomy_term_data'
    load_url_aliases_query = "SELECT pid, source, alias FROM url_alias"

    def load_drupal_entities(self, connection, model_class, drupal_table_name, column_map_list, page_model, alias_model, resolver, page_matcher=None):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        query = "SELECT id, {columns} FROM {drupal_table_name}"
        columns = ", ".join([c.drupal_name for c in column_map_list])

        cursor.execute(query.format(columns=columns, drupal_table_name=drupal_table_name))
        for values in cursor:
            eid = values[0]
            entity, created = model_class.objects.get_or_create(eid=eid)

            values = values[1:] # discard the id
            for i, column in enumerate(column_map_list):
                if column.type_or_map == 'naive_datetime':
                    value = make_aware(values[i], pytz.utc)
                elif column.type_or_map == 'timestamp':
                    value = datetime.fromtimestamp(values[i])
                #TODO: if callable call it
                else:
                    value = values[i]

                setattr(entity, column.model_name, value)

            entity.save()

            # TODO: ??
            if page_matcher:
                page_matcher(entity, page_model, alias_model, resolver)
            else:
                self.match_entity_to_pages(entity, page_model, alias_model, resolver)

            if created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        print("%s: Added %d, Update %d" % (model_class.__name__, added_count, updated_count))


    def load_drupal_nodes(self, connection, model_class, node_type_name, page_model, alias_model, page_matcher=None):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        query = "SELECT n.nid, n.vid, n.title, n.status, n.created, n.changed "\
                "FROM  node n "\
                "WHERE n.type = '%s' " % (node_type_name)

        cursor.execute(query)
        for values in cursor:
            nid = values[0]
            vid = values[1]
            title = values[2]
            status = values[3]
            created_ts = values[4]
            changed_ts = values[5]

            node, node_created = model_class.objects.get_or_create(nid=nid)
            node.vid = vid
            node.title = title
            node.status = status
            node.created = datetime.fromtimestamp(created_ts)
            node.changed = datetime.fromtimestamp(changed_ts)

            node.save()

            if page_matcher:
                page_matcher(node, page_model, alias_model)
            else:
                self.match_to_pages(node, page_model, alias_model)


            if node_created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        print("%s: Added %d, Update %d" % (model_class.__name__, added_count, updated_count))

    def load_linked_data_field(
        self,
        connection,
        content_type,
        node_type_name,
        linked_content_field_name,
        linked_content_field_columns,
        linker
    ):

        linked_nodes = 0
        unlinked_nodes = 0

        cursor = connection.cursor()

        query = """
SELECT f.entity_id{linked_content_field_columns}
FROM field_data_{linked_content_field_name} f
WHERE f.bundle = '{node_type_name}'
"""
        query = query.format(
            linked_content_field_columns=", f.%s" % ", f.".join(linked_content_field_columns),
            linked_content_field_name=linked_content_field_name,
            node_type_name=node_type_name,
        )

        cursor.execute(query)
        for data in cursor:
            nid = data[0]
            data_values = data[1:]
            ct_object = content_type.objects.get(nid=nid)

            linker(ct_object, data_values)
            ct_object.save()

        cursor.close()

    @staticmethod
    def match_entity_to_pages(entity, page_model, alias_model, resolver):
        main_src, extra_srcs = resolver(entity)

        for src in [main_src] + extra_srcs:
            page, created = page_model.objects.get_or_create(page_path=src)
            entity.pages.add(page)

        # TODO: How should we handle alaises
        aliases = alias_model.objects.filter(src=main_src)
        for alias in aliases:
            entity.aliases.add(alias)

            page_path = "/%s" % alias.dst
            page, created = page_model.objects.get_or_create(page_path=page_path)
            entity.pages.add(page)

            page_path = "/%s/" % alias.dst
            page, created = page_model.objects.get_or_create(page_path=page_path)
            entity.pages.add(page)


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--app', '-s', dest='app', help='App name corresponding to Drupal site.'),
    )
    help = 'Imports drupal data'

    def handle(self, **options):
        app = options['app']
        app_module = importlib.import_module(app)

        importer = app_module.Importer(app)

        importer.open_connection()
        importer.handle_import()
        importer.close_connection()
