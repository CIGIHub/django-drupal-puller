from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils.timezone import utc, make_aware
from datetime import datetime
from collections import namedtuple
from optparse import make_option


import MySQLdb
import importlib
import re
import pytz


verbosity = 1


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
        self.connection = MySQLdb.connect(**config)

    def close_connection(self):
        self.connection.close()

    def get_database_configuration(self):
        return settings.SITE_DATABASE_CONFIG[self.site_name]

    def load_terms(self, model_class, connection):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()
        query = "SELECT tid, name FROM {0} WHERE vid=%s".format(self.taxonomy_term_data_table_name)

        vocabulary_id = model_class.vocabulary_id if hasattr(model_class, 'vocabulary_id') else model_class.vocabulary_id()

        cursor.execute(query, (vocabulary_id,))
        results = cursor.fetchall()
        for (tid, name) in results:
            term, created = model_class.objects.get_or_create(source_id=tid)
            term.name = name
            term.save()

            if created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        if verbosity > 1: print("%ss: Added %d, Update %d" % (model_class.__name__, added_count, updated_count))

    def load_url_aliases(self, connection, alias_model):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        query = self.load_url_aliases_query
        cursor.execute(query)
        results = cursor.fetchall()
        for (pid, src, dst) in results:
            alias, created = alias_model.objects.get_or_create(pid=pid)
            alias.src = src
            alias.dst = dst
            alias.save()

            if created:
                added_count += 1
            else:
                updated_count += 1

        cursor.close()

        if verbosity > 1: print("Url Aliases: Added %d, Update %d" % (added_count, updated_count))

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
        results = cursor.fetchall()
        for values in results:
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

        if verbosity > 1: print("%s: Added %d, Update %d" % (content_type.__name__, added_count, updated_count))

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
        results = cursor.fetchall()
        for (nid, vid, linked_nid) in results:
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

        if verbosity > 1: print("Linked Nodes: %s, Unlinked Nodes: %s" % (linked_nodes, unlinked_nodes))

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
        results = cursor.fetchall()
        for (nid, data_value) in results:
            ct_object = content_type.objects.get(nid=nid)

            linker(ct_object, data_value)
            ct_object.save()

        cursor.close()

        #if verbosity > 1: print "Unlinked Authors Updated"

    @staticmethod
    def match_to_pages(node, page_model, alias_model):
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
        results = cursor.fetchall()
        for values in results:
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

        if verbosity > 1: print("%s: Added %d, Update %d" % (model_class.__name__, added_count, updated_count))

    def load_drupal_nodes(self, connection, model_class, node_type_name, page_model, alias_model, page_matcher=None):
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        query = "SELECT n.nid, n.vid, n.title, n.status, n.created, n.changed "\
                "FROM  node n "\
                "WHERE n.type = '%s' " % (node_type_name)

        cursor.execute(query)
        results = cursor.fetchall()
        for values in results:
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

        if verbosity > 1: print("%s: Added %d, Update %d" % (model_class.__name__, added_count, updated_count))

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
        results = cursor.fetchall()
        for data in results:
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


def string_converter(value):
    return value.decode('latin1').strip()


def datetime_converter(value):
    value = value.strip()
    if value != '':
        return datetime.strptime(
            value,
            "%Y-%m-%dT%H:%M:%S",
        ).replace(tzinfo=utc)
    else:
        return None


def person_names_converter(names):
    names = string_converter(names)
    cleaned_names = names.replace("et al.", "").replace("Edited by", "")
    name_list = re.split(',|\s+and\s+|\s+with\s+', cleaned_names)

    def parse_name(name):
        name_parts = name.strip().rsplit(None, 1)
        return name_parts

    return [parse_name(name) for name in name_list]


def reference_converter(value):
    return int(value)


TFieldSpec = namedtuple('FieldSpec', ['name', 'field_type', 'default'])


def FieldSpec(name, field_type='string', default=''):
    return TFieldSpec(name, field_type, default)


class Drupal8BaseImporter(BaseImporter):
    taxonomy_term_data_table_name = 'taxonomy_term_field_data'
    load_url_aliases_query = "SELECT pid, source, alias FROM url_alias"

    field_type_converters = {
        'string': string_converter,
        'datetime': datetime_converter,
        'person_names': person_names_converter,
        'reference': reference_converter,
    }

    def load_drupal_nodes(self, connection, model_class, node_type_name, page_model, alias_model, page_matcher=None):
        '''
        I think I am going to chnage the flow here...
        After you load nodes you can link addition data...
        '''
        added_count = 0
        updated_count = 0
        cursor = connection.cursor()

        query = "SELECT n.nid, n.vid, n.title, n.status, n.created, n.changed "\
                "FROM  node_field_data n "\
                "WHERE n.type = '%s' " % (node_type_name)

        cursor.execute(query)
        results = cursor.fetchall()
        for values in results:
            nid = values[0]
            vid = values[1]
            title = values[2]
            status = values[3]
            created_ts = values[4]
            changed_ts = values[5]

            node, node_created = model_class.objects.get_or_create(nid=nid)
            node.vid = vid
            node.title = string_converter(title)
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

    def get_node_field_data(self, connection, bundle_name, specs):
        '''
        Grab all the data and return a dictionary in the format {entity_id: { spec.name: value }}
        for all of the FieldSpecs in specs. The value has been converted based on field_type.
        '''
        ret = {}

        for spec in specs:
            cursor = connection.cursor()

            value_field_template = 'field_{field_name}_value'
            if spec.field_type == 'reference':
                value_field_template = 'field_{field_name}_target_id'

            value_field_name = value_field_template.format(field_name=spec.name)

            query = """
SELECT f.entity_id, f.{value_field_name}
FROM node__field_{field_name} f
WHERE f.bundle = '{bundle_name}'
ORDER BY entity_id, delta
"""
            query = query.format(
                value_field_name=value_field_name,
                field_name=spec.name,
                bundle_name=bundle_name,
            )

            cursor.execute(query)
            results = cursor.fetchall()
            for values in results:
                nid = values[0]
                value = values[1]

                if nid not in ret:
                    default_value = dict()

                    for s in specs:
                        default = s.default
                        if callable(default):
                            default = default()
                        default_value[s.name] = default

                    ret[nid] = default_value

                if spec.field_type in self.field_type_converters:
                    value = self.field_type_converters[spec.field_type](value)

                if isinstance(ret[nid][spec.name], list):
                    ret[nid][spec.name].append(value)
                else:
                    ret[nid][spec.name] = value

        return ret

    def get_taxonomy_data(self, connection, bundle_name, term_model, is_field=False):
        '''
        Return dict {nid: [term_instance,...]}
        Callers job to know what context the nid should be in, can be reused.
        '''
        ret = {}
        cursor = connection.cursor()

        field_template = "{vocabulary_id}_target_id"
        if is_field:
            field_template = "field_{vocabulary_id}_target_id"
        field_name = field_template.format(
            vocabulary_id=term_model.vocabulary_id
        )

        table_template = "node__{vocabulary_id}"
        if is_field:
            table_template = "node__field_{vocabulary_id}"
        table_name = table_template.format(
            vocabulary_id=term_model.vocabulary_id
        )

        query = """
SELECT t.entity_id, t.{field_name}
FROM {table_name} t
WHERE t.bundle = '{bundle_name}'
"""

        query = query.format(
            field_name=field_name,
            table_name=table_name,
            bundle_name=bundle_name,
        )

        cursor.execute(query)
        results = cursor.fetchall()
        for values in results:
            nid, tid = values
            term_instance = term_model.objects.get(source_id=tid)

            if nid not in ret:
                ret[nid] = []

            ret[nid].append(term_instance)

        return ret

    @staticmethod
    def match_to_pages(node, page_model, alias_model):
        src = "/node/%d" % node.nid

        page, created = page_model.objects.get_or_create(page_path=src)
        node.pages.add(page)

        page, created = page_model.objects.get_or_create(page_path="%s/" % src)
        node.pages.add(page)

        aliases = alias_model.objects.filter(src=src)
        for alias in aliases:
            node.aliases.add(alias)

            page_path = "%s" % alias.dst
            page, created = page_model.objects.get_or_create(page_path=page_path)
            node.pages.add(page)

            page_path = "%s/" % alias.dst
            page, created = page_model.objects.get_or_create(page_path=page_path)
            node.pages.add(page)


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option(
            '--app',
            '-s',
            dest='app',
            help='App name corresponding to Drupal site.'
        ),
    )
    help = 'Imports drupal data'

    def handle(self, **options):
        app = options['app']

        global verbosity
        verbosity = options['verbosity']

        app_module = importlib.import_module(app)

        importer = app_module.Importer(app)

        importer.open_connection()
        importer.handle_import()
        importer.close_connection()
