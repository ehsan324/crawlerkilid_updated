# Generated by Django 5.0.6 on 2024-06-06 09:41

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Listing',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('image_url', models.URLField()),
                ('image', models.ImageField(blank=True, null=True, upload_to='listing_images/')),
                ('price', models.CharField(max_length=100)),
                ('facilities', models.TextField()),
                ('address', models.TextField()),
            ],
        ),
    ]
