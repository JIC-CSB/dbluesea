"""dbluesea CLU"""

import os
import json
import errno

import click

from dtoolcore import DataSet

from azure.storage.blob import BlockBlobService

import config


def mkdir_parents(path):
    """Create the given directory path.
    This includes all necessary parent directories. Does not raise an error if
    the directory already exists.
    :param path: path to create
    """

    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise


@click.group()
def cli():
    pass


@cli.command()
@click.argument('dataset_path', type=click.Path(exists=True))
def put(dataset_path):
    dataset = DataSet.from_path(dataset_path)

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    ret = block_blob_service.create_container(dataset.uuid)

    block_blob_service.create_blob_from_text(
        dataset.uuid,
        'manifest',
        json.dumps(dataset.manifest)
    )

    for identifier in dataset.identifiers:
        block_blob_service.create_blob_from_path(
            dataset.uuid,
            identifier,
            dataset.abspath_from_identifier(identifier)
        )


@cli.command()
def show(uuid="4b4f06cc-72b9-4487-803f-6c5ac269af5e"):

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    generator = block_blob_service.list_blobs(uuid)
    for blob in generator:
        print(blob.name)


@cli.command()
def get(uuid="4b4f06cc-72b9-4487-803f-6c5ac269af5e"):

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    manifest_blob = block_blob_service.get_blob_to_text(
        uuid,
        'manifest'
    )

    manifest = json.loads(manifest_blob.content)

    for item in manifest['file_list']:
        path, basename = os.path.split(item['path'])

        if len(path):
            mkdir_parents(path)

        block_blob_service.get_blob_to_path(
            uuid,
            item['hash'],
            item['path']
        )
