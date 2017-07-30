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

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    block_blob_service.create_container(dataset.uuid)
    block_blob_service.set_container_metadata(
        dataset.uuid,
        dataset._admin_metadata
    )

    block_blob_service.create_blob_from_text(
        dataset.uuid,
        'dtool',
        json.dumps(dataset._admin_metadata))

    block_blob_service.create_blob_from_text(
        dataset.uuid,
        'manifest',
        json.dumps(dataset.manifest)
    )

    block_blob_service.create_blob_from_path(
        dataset.uuid,
        'README.yml',
        dataset.abs_readme_path
    )

    for identifier in dataset.identifiers:
        if not block_blob_service.exists(dataset.uuid, identifier):
            block_blob_service.create_blob_from_path(
                dataset.uuid,
                identifier,
                dataset.abspath_from_identifier(identifier)
            )


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
        metadata = blob.metadata
        if 'path' in metadata:
            path = metadata['path']
        else:
            path = ""

        print("{} {}".format(blob.name, path))


@cli.command()
@click.argument(
    "uuid",
    default="4b4f06cc-72b9-4487-803f-6c5ac269af5e"
)
def get(uuid):

    block_blob_service = BlockBlobService(
        account_name=config.STORAGE_ACCOUNT_NAME,
        account_key=config.STORAGE_ACCOUNT_KEY
    )

    admin_metadata = block_blob_service.get_container_metadata(uuid)

    dataset_name = admin_metadata['name']

    dataset_root_path = dataset_name
    dtool_dir_path = os.path.join(dataset_root_path, '.dtool')
    mkdir_parents(dtool_dir_path)

    dtool_file_path = os.path.join(dtool_dir_path, 'dtool')
    with open(dtool_file_path, 'w') as fh:
        fh.write(json.dumps(admin_metadata))

    manifest_blob = block_blob_service.get_blob_to_text(
        uuid,
        'manifest'
    )
    manifest_file_path = os.path.join(
        dataset_root_path,
        admin_metadata['manifest_path']
    )
    with open(manifest_file_path, 'w') as fh:
        fh.write(manifest_blob.content)

    manifest = json.loads(manifest_blob.content)

    manifest_root_path = os.path.join(
        dataset_root_path,
        admin_metadata['manifest_root']
    )

    for item in manifest['file_list']:
        dest_full_path = os.path.join(manifest_root_path, item['path'])

        if not os.path.exists(dest_full_path):

            dest_path, dest_filename = os.path.split(dest_full_path)

            if len(dest_path):
                mkdir_parents(dest_path)

            block_blob_service.get_blob_to_path(
                uuid,
                item['hash'],
                dest_full_path
            )

    block_blob_service.get_blob_to_path(
        uuid,
        'README.yml',
        os.path.join(dataset_root_path, admin_metadata['readme_path'])
    )
