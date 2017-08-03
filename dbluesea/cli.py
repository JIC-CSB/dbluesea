"""dbluesea CLU"""

import os
import json
import errno

import click

from dtoolcore import DataSet

from azure.storage.blob import BlockBlobService

import dbluesea.config as config
from dbluesea import AzureDataSet


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
@click.argument('name')
def new(name):
    dataset = AzureDataSet(name)
    dataset.persist_to_azure()
    print(dataset.uuid)


@cli.command()
@click.argument('uuid')
def update(uuid):
    dataset = AzureDataSet.from_uuid(uuid)
    print('Updating: {}'.format(uuid))
    dataset.update_manifest()


@cli.command()
def list():
    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    containers = block_blob_service.list_containers(include_metadata=True)

    for c in containers:
        print(' '.join((c.name, c.metadata['name'])))


@cli.command()
@click.argument('dataset_path', type=click.Path(exists=True))
def putoverlays(dataset_path):

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    dataset = DataSet.from_path(dataset_path)

    overlays_abspath = os.path.join(
        dataset._abs_path,
        dataset._admin_metadata["overlays_path"]
    )

    for overlay_file in os.listdir(overlays_abspath):
        overlay_abspath = os.path.join(overlays_abspath, overlay_file)

        block_blob_service.create_blob_from_path(
            dataset.uuid,
            overlay_file,
            overlay_abspath
        )


@cli.command()
@click.argument('dataset_path', type=click.Path(exists=True))
def put(dataset_path):
    dataset = DataSet.from_path(dataset_path)

    remote_dataset = AzureDataSet(dataset.name)
    remote_dataset._admin_metadata = dataset._admin_metadata

    with open(dataset.abs_readme_path) as fh:
        remote_dataset.persist_to_azure(readme=fh.read())

    for identifier in dataset.identifiers:
        path = dataset.item_from_identifier(identifier)['path']
        remote_dataset.put_from_local_path(
            dataset.abspath_from_identifier(identifier),
            path
        )

    remote_dataset.update_manifest()


@cli.command()
@click.argument(
    "uuid",
    default="4b4f06cc-72b9-4487-803f-6c5ac269af5e"
)
def show(uuid):

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    generator = block_blob_service.list_blobs(uuid, include='metadata')
    for blob in generator:
        if 'path' in blob.metadata:
            path = blob.metadata['path']
        else:
            path = ""

        print("{} {} {}".format(
            blob.name,
            path,
            blob.properties.content_length
        ))


@cli.command()
@click.argument("uuid")
def get(uuid):

    dataset = AzureDataSet.from_uuid(uuid)

    local_dataset = DataSet(dataset.name)
    local_dataset._admin_metadata = dataset._admin_metadata
    local_dest_path = dataset.name
    os.mkdir(local_dest_path)
    local_dataset.persist_to_path(local_dest_path)

    with open(local_dataset.abs_readme_path, 'w') as fh:
        fh.write(dataset.readme)

    for identifier in dataset.identifiers:
        path = dataset.item_from_identifier(identifier)['path']
        dest_abspath = os.path.join(
            local_dest_path,
            local_dataset.data_directory,
            path
        )
        dest_path, dest_filename = os.path.split(dest_abspath)

        if len(dest_path):
            mkdir_parents(dest_path)

        dataset.stage_to_local_path(identifier, dest_abspath)

    local_dataset.update_manifest()


@cli.command()
@click.argument("uuid")
@click.argument("identifier")
def fetch(uuid, identifier):
    """Fetch the item with the given identifier from the Azure dataset with
    the given UUID. The item will be written to the current directory."""

    dataset = AzureDataSet.from_uuid(uuid)

    path = dataset.item_from_identifier(identifier)["path"]

    filename = os.path.basename(path)

    dataset.stage_to_local_path(identifier, filename)


@cli.command()
@click.argument("uuid")
def rm(uuid):
    """Remove the Azure dataset with the given UUID."""

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    block_blob_service.delete_container(uuid)
