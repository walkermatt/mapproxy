from __future__ import with_statement
import os
import errno

from mapproxy.image import ImageSource
from mapproxy.cache.base import tile_buffer
from mapproxy.cache.file import FileCache

import boto
import StringIO
from mapproxy.util import async
from threading import Timer

import logging
log = logging.getLogger('mapproxy.cache.s3')

class S3Cache(FileCache):
    """
    This class is responsible to store and load the actual tile data.
    """
    def __init__(self, cache_dir, file_ext, lock_dir=None, directory_layout='tc',
                 lock_timeout=60.0, bucket='mapproxy'):
        """
        :param cache_dir: the path where the tile will be stored
        :param file_ext: the file extension that will be appended to
            each tile (e.g. 'png')
        """
        super(S3Cache, self).__init__(cache_dir, file_ext=file_ext, directory_layout=directory_layout, lock_timeout=lock_timeout, link_single_color_images=False)

        self.s3_conn = boto.connect_s3()

        try:
            self.bucket = self.s3_conn.get_bucket(bucket)
        except boto.exception.S3ResponseError, e:
            if e.error_code == 'NoSuchBucket':
                self.bucket = self.s3_conn.create_bucket(bucket, location=boto.s3.connection.Location.EU)

    def load_tile_metadata(self, tile):
        # TODO Implement storing / retrieving tile metadata
        tile.timestamp = 0
        tile.size = 0

    def is_cached(self, tile):
        """
        Returns ``True`` if the tile data is present.
        """
        if tile.is_missing():

            location = self.tile_location(tile)
            log.debug('is_cached, location: %s' % location)

            k = boto.s3.key.Key(self.bucket)
            k.key = location
            if k.exists():
                return True
            else:
                return False
        else:
            return True

    def load_tile(self, tile, with_metadata=False):
        """
        Fills the `Tile.source` of the `tile` if it is cached.
        If it is not cached or if the ``.coord`` is ``None``, nothing happens.
        """
        if not tile.is_missing():
            return True

        location = self.tile_location(tile)
        log.debug('load_tile, location: %s' % location)

        tile_data = StringIO.StringIO()
        k = boto.s3.key.Key(self.bucket)
        k.key = location
        try:
            k.get_contents_to_file(tile_data)
            tile.source = ImageSource(tile_data)
            k.close()
            return True
        except boto.exception.S3ResponseError, e:
            # NoSuchKey
            pass
        k.close()
        return False

    def remove_tile(self, tile):

        location = self.tile_location(tile)
        log.debug('remove_tile, location: %s' % location)

        k = boto.s3.key.Key(self.bucket)
        k.key = location
        if k.exists():
            k.delete()
        k.close()

    def store_tile(self, tile):
        """
        Add the given `tile` to the file cache. Stores the `Tile.source` to
        `FileCache.tile_location`.
        """
        if tile.stored:
            return

        location = self.tile_location(tile)
        log.debug('store_tile, location: %s' % location)

        k = boto.s3.key.Key(self.bucket)
        k.key = location
        with tile_buffer(tile) as buf:
            k.set_contents_from_file(buf)
        k.close()

        # Attempt making storing tiles non-blocking

        # This is still blocking when I thought that it would not
        # async.run_non_blocking(self.async_store, (k, tile))

        # async_pool = async.Pool(4)
        # for store in async_pool.map(self.async_store_, [(k, tile)]):
        #     log.debug('stored...')

        # This sometimes suffers from "ValueError: I/O operation on closed file"
        # as I guess it's not advised to use threads within a wsgi app
        # Timer(0.25, self.async_store, args=[k, tile]).start()

    def async_store_(self, foo):
        key, tile = foo
        print 'Storing %s, %s' % (key, tile)
        with tile_buffer(tile) as buf:
            key.set_contents_from_file(buf)

    def async_store(self, key, tile):
        print 'Storing %s, %s' % (key, tile)
        with tile_buffer(tile) as buf:
            key.set_contents_from_file(buf)

