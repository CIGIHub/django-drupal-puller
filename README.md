django-drupal-puller
====================

Django app used to pull drupal content into django database. It provides abstract models which you should
inherit from in your app. It also provides base importer functionality which should be extended to handle 
your data structures.

To Use
------

Add the app to your settings.py file:

    INSTALLED_APPS = [...
                      'drupal_puller',
                      ...]
                      
In your app,

Create a drupal_import.py.  Here is a sample of a basic implementation:

    from drupal_puller.management.commands.drupal_import import Drupal7BaseImporter, column_map
    from .models import Subject, Partner
    
    class Importer(Drupal7BaseImporter):
    
        def handle_import(self, **options):
            self.load_terms(Topic, self.connection)
            self.load_partners(self.connection)
        
            
        def load_partners(self, connection):

            def partner_url_linker(ct_object, values):
                url_value = values[0]
    
                if url_value:
                    ct_object.url_value = url_value

            self.load_drupal_nodes(connection, Partner, "partner", Page, DrupalUrlAlias)
            self.load_linked_data_field(connection, Partner, "partner", "partner_url", ["partner_url_url"], partner_url_linker)


Create models that inherit from the abstract classes provided.  Here is a sample of a basic implementation:

    from drupal_puller.models import DrupalEntity, DrupalNode, TaxonomyTerm, DrupalUrlAliasBase


    class Partner(DrupalNode):
        url_value = models.CharField(max_length=512, null=True)
    
        def __unicode__(self):
            return "%s" % self.title

    
    class Subject(TaxonomyTerm):
        @classmethod
        def vocabulary_id(cls):
            return 2
    
    class DrupalUrlAlias(DrupalUrlAliasBase):
        pass


Add the Importer to the __init__.py of your app module:

    from .drupal_import import Importer
    

Add django settings:
    
    db1_config = {
        'user': '<user>',
        'passwd': '<password>',
        'host': '<host>',
        'db': '<dbname>',
    }
    
    db2_config = {
        'user': '<user>',
        'passwd': '<password>',
        'host':'<host>',
        'db':'<dbname>',
    }
    
    site_database_config = {
        'app1': db1_config,
        'app2': db2_config,
    }
    
