from django.db import models


class DrupalEntity(models.Model):
    eid = models.IntegerField()

    pages = models.ManyToManyField('Page')
    aliases = models.ManyToManyField('DrupalUrlAlias')

    class Meta:
        abstract = True


class DrupalNode(models.Model):
    nid = models.IntegerField()
    vid = models.IntegerField(null=True)
    #type = models.CharField(max_length=32)
    title = models.CharField(max_length=255, null=True)
    #uid = models.IntegerField()
    status = models.IntegerField(null=True)
    created = models.DateField(null=True)
    changed = models.DateField(null=True)
    #promote = models.IntegerField()

    pages = models.ManyToManyField('Page')
    aliases = models.ManyToManyField('DrupalUrlAlias')

    #related_pages = models.ManyToManyField('Page', blank=True, null=True, related_name='related_%(app_label)s_%(class)s')
    def __unicode__(self):
        return "%s" % self.title

    class Meta:
        abstract = True


class TaxonomyTerm(models.Model):
    name = models.CharField(max_length=150, null=True)
    source_id = models.IntegerField()
    #term_data

    @classmethod
    def vocabulary_id(cls):
        raise NotImplementedError()

    def __unicode__(self):
        return "%s - %s" % (self.source_id, self.name)

    class Meta:
        abstract = True


class DrupalUrlAliasBase(models.Model):
    pid = models.IntegerField()
    src = models.CharField(max_length=128, null=True)
    dst = models.CharField(max_length=128, null=True)

    def __unicode__(self):
        return "%s" % self.dst

    class Meta:
        abstract = True
