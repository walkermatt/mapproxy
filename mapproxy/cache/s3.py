from __future__ import with_statement
import os
import errno

from mapproxy.util import ensure_directory
from mapproxy.image import ImageSource, is_single_color_image
from mapproxy.cache.base import TileCacheBase, FileBasedLocking, tile_buffer
from mapproxy.cache.file import FileCache

import boto
import StringIO

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
        self.bucket = bucket

        log.info('bucket: %s' % self.bucket)

    def load_tile_metadata(self, tile):
        location = self.tile_location(tile)
        try:
            stats = os.lstat(location)
            tile.timestamp = stats.st_mtime
            tile.size = stats.st_size
        except OSError, ex:
            if ex.errno != errno.ENOENT: raise
            tile.timestamp = 0
            tile.size = 0
    
    def is_cached(self, tile):
        """
        Returns ``True`` if the tile data is present.
        """
        return False
        if tile.is_missing():
            location = self.tile_location(tile)
            if os.path.exists(location):
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

        conn = boto.connect_s3()
        tile_data = StringIO.StringIO()
        b = conn.create_bucket(self.bucket)
        k = boto.s3.key.Key(b)
        log.info(location)
        k.key = location
        k.set_contents_from_file(tile_data)
        tile.source = ImageSource(tile_data)
        return True

        # if os.path.exists(location):
        #     if with_metadata:
        #         self.load_tile_metadata(tile)
        #     tile.source = ImageSource(location)
        #     return True
        # return False

    def remove_tile(self, tile):
        location = self.tile_location(tile)
        try:
            os.remove(location)
        except OSError, ex:
            if ex.errno != errno.ENOENT: raise

    def store_tile(self, tile):
        """
        Add the given `tile` to the file cache. Stores the `Tile.source` to
        `FileCache.tile_location`.
        """
        if tile.stored:
            return
        
        tile_loc = self.tile_location(tile)
        
        log.info('tile_loc %s' % tile_loc)
        conn = boto.connect_s3()
        b = conn.create_bucket(self.bucket)
        k = boto.s3.key.Key(b)
        k.key = tile_loc
        with tile_buffer(tile) as buf:
            tile_data = buf.getvalue()
            k.set_contents_from_file(buf)
