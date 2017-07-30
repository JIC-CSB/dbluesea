"""dbluesea package."""

import os
import json
import uuid
import hashlib

from azure.storage.blob import BlockBlobService

import dbluesea.config as config


__version__ = "0.1.0"


AZURECACHE = '/Users/hartleym/azurecache'


def hashsum(hasher, filename):
    """Helper function for creating hash functions.

    See implementation of :func:`dtoolcore.filehasher.shasum`
    for more usage details.
    """
    BUF_SIZE = 65536
    with open(filename, 'rb') as f:
        buf = f.read(BUF_SIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(BUF_SIZE)
    return hasher.hexdigest()


def shasum(filename):
    """Return hex digest of SHA-1 hash of file.

    :param filename: path to file
    :returns: shasum of file
    """
    hasher = hashlib.sha1()
    return hashsum(hasher, filename)


class AzureDataSet(object):

    def __init__(self, name):
        self._manifest = None
        self._block_blob_service = None

        self._admin_metadata = {
            "uuid": str(uuid.uuid4()),
            "dbluesea_version": __version__,
            "readme_path": "README.yml",
            "manifest_path": os.path.join(".dtool", "manifest.json"),
            "overlays_path": os.path.join(".dtool", "overlays"),
            "manifest_root": "data",
            "name": name,
        }

        self.local_cache_path = AZURECACHE

    @property
    def name(self):
        """Return the name of the dataset."""
        return self._admin_metadata['name']

    @property
    def uuid(self):
        """Return the dataset's UUID."""
        return self._admin_metadata['uuid']

    @property
    def block_blob_service(self):
        if self._block_blob_service is None:
            self._block_blob_service = BlockBlobService(
                account_name=config.STORAGE_ACCOUNT_NAME,
                account_key=config.STORAGE_ACCOUNT_KEY
            )
        return self._block_blob_service

    def persist_to_azure(self, readme=""):
        self.block_blob_service.create_container(self.uuid)
        self.block_blob_service.set_container_metadata(
            self.uuid,
            self._admin_metadata
        )

        self._manifest = {
            "file_list": [],
            "dbluesea_version": __version__,
            "hash_function": "shasum"
        }
        self.store_manifest()

        self.block_blob_service.create_blob_from_text(
            self.uuid,
            'README.yml',
            readme
        )

    @classmethod
    def from_uuid(cls, uuid):

        block_blob_service = BlockBlobService(
            account_name=config.STORAGE_ACCOUNT_NAME,
            account_key=config.STORAGE_ACCOUNT_KEY
        )

        admin_metadata = block_blob_service.get_container_metadata(uuid)

        dataset = cls(admin_metadata['name'])
        dataset._admin_metadata = admin_metadata
        dataset._block_blob_service = block_blob_service

        return dataset

    @property
    def manifest(self):

        if self._manifest is None:
            manifest_blob = self.block_blob_service.get_blob_to_text(
                self.uuid,
                'manifest'
            )
            self._manifest = json.loads(manifest_blob.content)

        return self._manifest

    def update_manifest(self):
        blob_list = self.block_blob_service.list_blobs(
            self.uuid,
            include='metadata'
        )

        file_list = []
        for blob in blob_list:
            metadata = blob.metadata
            if 'path' in metadata:
                new_entry = {
                    'hash': blob.name,
                    'path': metadata['path']
                }
                file_list.append(new_entry)

        new_manifest = {
            "file_list": file_list,
            "dbluesea_version": __version__,
            "hash_function": "shasum"
        }
        self._manifest = new_manifest
        self.store_manifest()

    def store_manifest(self):
        lease = self.block_blob_service.acquire_container_lease(self.uuid, 15)
        res = self.block_blob_service.create_blob_from_text(
            self.uuid,
            'manifest',
            json.dumps(self.manifest)
        )
        self.block_blob_service.break_container_lease(self.uuid)

    def item_from_identifier(self, identifier):
        """Return an item of a dataset based on it's identifier.

        :param identifier: dataset item identifier
        :returns: dataset item as a dictionary
        """
        for item in self.manifest["file_list"]:
            if item["hash"] == identifier:
                return item
        raise(KeyError("File hash not in dataset"))

    @property
    def identifiers(self):
        """Return list of dataset item identifiers."""
        file_list = self.manifest["file_list"]
        return [item["hash"] for item in file_list]

    def stage_to_local_path(self, identifier, local_path):

        self.block_blob_service.get_blob_to_path(
            self.uuid,
            identifier,
            local_path
        )

    def abspath_from_identifier(self, identifier):

        cache_path = os.path.join(self.local_cache_path, identifier)

        original_path = self.item_from_identifier(identifier)['path']
        _, original_ext = os.path.splitext(original_path)

        cache_path = cache_path + original_ext

        if not os.path.isfile(cache_path):
            self.stage_to_local_path(identifier, cache_path)

        return cache_path


    def access_overlay(self, overlay_name):

        overlay_filename = overlay_name + '.json'

        raw_overlay = self.block_blob_service.get_blob_to_text(
            self.uuid,
            overlay_filename
        )

        return json.loads(raw_overlay.content)


    def put_from_local_path(self, local_path, path):
        # path is the path metadata that will be attached to the file. This
        # becomes important when the dataset is copied to local storage

        identifier = shasum(local_path)

        self.block_blob_service.create_blob_from_path(
            self.uuid,
            identifier,
            local_path
        )

        self.block_blob_service.set_blob_metadata(
            container_name=self.uuid,
            blob_name=identifier,
            metadata={"path": path}
        )
