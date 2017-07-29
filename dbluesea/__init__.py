"""dbluesea package."""

import os
import json

from azure.storage.blob import BlockBlobService

import dbluesea.config as config


__version__ = "0.1.0"


AZURECACHE = '/Users/hartleym/azurecache'

class AzureDataSet(object):

    def __init__(self, uuid):
        self._manifest = None
        self.uuid = uuid
        self.local_cache_path = AZURECACHE

    @classmethod
    def from_uuid(cls, uuid):
        dataset = cls(uuid)

        dataset.block_blob_service = BlockBlobService(
            account_name=config.STORAGE_ACCOUNT_NAME,
            account_key=config.STORAGE_ACCOUNT_KEY
        )

        dtool_file_blob = dataset.block_blob_service.get_blob_to_text(
            uuid,
            'dtool'
        )

        dataset._admin_metadata = json.loads(dtool_file_blob.content)

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

        if not os.path.isfile(cache_path):
            self.stage_to_local_path(self, identifier, cache_path)

        return cache_path
