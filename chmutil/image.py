# -*- coding: utf-8 -*-

import os
import math
import logging
from PIL import Image
from PIL import ImageMath


logger = logging.getLogger(__name__)


class InvalidImageError(Exception):
    """Denotes invalid image object
    """
    pass


class InvalidImageDirError(Exception):
    """Raised when invalid image directory is used
    """
    pass


def get_image_path_list(image_dir, suffix,
                        keysortfunc=None):
    """Gets list of images with suffix from dir
    :param image_dir: Path to directory with images
    :param suffix: Only include files ending with suffix.
                   code uses str().endswidth for checking.
                   If `suffix` is None then all files match
    :raises InvalidImageDirError: if `image_dir` is None or not
                                  a directory
    :raises OSError: if there is an error invoking os.listdir on `image_dir`
    :returns: list of file paths
    """
    if image_dir is None:
        raise InvalidImageDirError('image_dir is None')

    if not os.path.isdir(image_dir):
        raise InvalidImageDirError('image_dir must be a directory')

    if suffix is None:
        adjusted_suffix = ''
    else:
        adjusted_suffix = suffix

    img_list = []
    for entry in os.listdir(image_dir):
        fp = os.path.join(image_dir, entry)
        if not os.path.isfile(fp):
            logger.debug(entry + ' is not a file. skipping')
            continue
        if entry.endswith(adjusted_suffix):
            img_list.append(fp)

    if keysortfunc is not None:
        logger.debug('Sort function passed in sorting data')
        img_list.sort(key=keysortfunc)

    return img_list


class SimpleImageMerger(object):
    """Merges two same size images together by taking maximum
    pixel value from either image
    """
    def __init__(self):
        """Constructor
        """
        pass

    def merge_images(self, image_list):
        """Merge list of images
        :param image_list: List of full path to image files to merge
        :return: Pillow Image containing merge of all images
        """
        if image_list is None:
            logger.error('No images to merge')
            return None
        logger.info('Found ' + str(len(image_list)) + ' images to merge')
        merged = None
        for entry in image_list:
            if merged is None:
                merged = Image.open(entry)
                continue
            logger.debug('Merging image ' + entry)
            merged = self._merge_two_images(merged, entry)
        return merged

    def _merge_two_images(self, image1, image_file2):
        """Merges two images together by taking max value of each
        pixel.
        :param image1: Pillow Image
        :param image_file2: Path to image
        :returns: Pillow image which is merge of image1 and image2 where
                  each pixel is max value found.
        """
        image2 = None
        try:
            image2 = Image.open(image_file2)
            logger.debug('Merging ' + image_file2)
            return ImageMath.eval("convert(max(a, b), 'L')", a=image1,
                                  b=image2)
        finally:
            if image2 is not None:
                image2.close()

            if image1 is not None:
                image1.close()


class ImageThresholder(object):
    """Thresholds image by percent specified
    """

    def __init__(self, threshold_percent=30, rawthreshold=None):
        """
        Constructor
        :param threshold_percent: int value ranging between 0 and 100 where
                                  0 is 0% and 100 is 100%
        :param rawcutoff: raw cutoff value setting to 3 means values 2 and
                          less to 0 and rest to 255
        """
        self._threshold_percent = threshold_percent
        if rawthreshold is not None:
            self._cutoff = rawthreshold
        else:
            self._cutoff = int((float(threshold_percent)*0.01)*255)

    def get_pixel_intensity_cutoff(self):
        """Gets pixel intensity cutoff as calculated in constructor
        :return: int denoting intensity cutoff where values below this
                 are set to 0 and values above are set to 255 in
                 `threshold_image` method
        """
        return self._cutoff

    def threshold_image(self, image):
        """Thresholds image passed in
        :param image: Image object from PIL to be thresholded
        :returns: Image object thresholded which is pointing to same
                  object image
        """
        if image is None:
            raise InvalidImageError('Image is None')

        return Image.eval(image, lambda px: 0 if px < self._cutoff else 255)


class ColorizeGrayscaleImage(object):
    """Takes an image that is grayscale and colorizes it by converting
       it to RGBA adjusting colors based on values set in constructor
    """

    def __init__(self, color=(1, 0, 0), opacity=150):
        """
        Constructor
        :param threshold_percent: int value ranging between 0 and 100 where
                                  0 is 0% and 100 is 100%
        :param opacity: 0 is transparent and 255 is opaque
        """
        self._color = color
        self._opacity = int(opacity)

    def get_color_tuple(self):
        """Gets colorizing tuple
        :return: tuple (R, G, B) where each value is a scale value
                 multiplied against input image values
        """
        return self._color

    def colorize_image(self, image):
        """Colorizes grayscale image
        :param image: Image object from PIL to be colorized
        :returns: Image object colorized of same size as input but of
                  type RGBA
        """
        if image is None:
            raise InvalidImageError('Image is None')

        red = Image.eval(image, lambda px: int(px*self._color[0]))
        green = Image.eval(image, lambda px: int(px*self._color[1]))
        blue = Image.eval(image, lambda px: int(px*self._color[2]))
        alpha = Image.eval(image, lambda px: 0 if px == 0 else self._opacity)

        resimage = Image.merge('RGBA', (red, green, blue, alpha))

        red.close()
        green.close()
        blue.close()
        alpha.close()
        return resimage


class ImageTile(object):
    """Represents a tile from a Pillow Image
    """
    def __init__(self, image, box=None, row=None,
                 col=None):
        """Constructor
        :param image: Pillow Image representing tile
        :param box: tuple (left, upper, right, lower) representing location of
                    tile in parent Image
        :param row: row tile belows to, expect int starting at 0
        :param col: column tile belows to, expect int starting at 0
        """
        self._image = image
        self._box = box
        self._row = row
        self._col = col

    def get_box(self):
        """Gets location of tile in parent image
        :returns: None or tuple (left, upper, right,lower)
        """
        return self._box

    def get_image(self):
        """Gets Image tile
        :returns: Pillow Image
        """
        return self._image

    def get_row(self):
        """Gets Row
        """
        return self._row

    def get_col(self):
        """Gets Column
        """
        return self._col


class RowColumnImageTileGenerator(object):
    """Generator that extracts `ImageTile` objects from Pillow Image
       where each tile is a square tile of size specified in
       constructor and i
    """
    def __init__(self, tilesize):
        """Constructor
        :param tilesize: size of tile in pixels as int ie 128
        """
        self._tilesize = tilesize

    def get_image_tiles(self, image):
        """Gets generator that obtains `ImageTile` from Pillow
           `image` passed in with the size of the tiles defined
           by `tilesize` in constructor
        :returns ImageTile: for each tile
        """
        if image is None:
            raise InvalidImageError('Image is None')

        (width, height) = image.size

        if width <= self._tilesize and height <= self._tilesize:
            logger.info('Tilesize is larger then image')
            yield ImageTile(image, (0, 0, width, height), row=0, col=0)
            return

        numrows = int(math.floor(height/self._tilesize))
        numcols = int(math.floor(width/self._tilesize))

        for c in range(numcols):
            for r in range(numrows):
                thebox = (c*self._tilesize,
                          r*self._tilesize,
                          c*self._tilesize + self._tilesize,
                          r*self._tilesize + self._tilesize)
                img = image.crop(thebox)
                yield ImageTile(img, box=thebox, row=r, col=c)


class SingleColumnImageTileGenerator(object):
    """Generator that extracts `ImageTile` objects from Pillow Image
       where each tile is always the width of the Pillow Image, but the
       height is set in the constructor
    """
    def __init__(self, tileheight=2000):
        """Constructor
        :param tileheight: int denoting height of tile in pixels.
        """
        self._tileheight = tileheight

    def get_image_tiles(self, image):
        """Gets generator that obtains single column of `ImageTile`
        from Pillow `image` passed in with the height set by
        tileheight variable in constructor and width of tile `image`
        :param image: Pillow image to tile
        :returns: Generator that returns `ImageTile` objects
        """
        if image is None:
            raise InvalidImageError('Image is None')

        (width, height) = image.size

        # if image is smaller then tile height then just return a copy
        # of image
        if height <= self._tileheight:
            logger.debug('image height ' + str(height) +
                         ' is smaller then tile height ' +
                         str(self._tileheight))
            yield ImageTile(image.copy(), box=(0, 0, width, height))
            return

        for offset in range(0, height, self._tileheight):
            if offset + self._tileheight <= height:
                cur_tile_height = offset + self._tileheight
            else:
                cur_tile_height = height
            box = (0, offset, width, cur_tile_height)
            logger.debug('Returning tile = ' + str(box))
            yield ImageTile(image.crop(box), box=box)


class ImageStats(object):
    """Contains information about an image to be segmented
    """
    def __init__(self, path, width, height, format,
                 size_in_bytes=0):
        """Constructor
        """
        self._path = path
        self._width = width
        self._height = height
        self._format = format
        self._size_in_bytes = size_in_bytes

    def get_width(self):
        """Gets width
        """
        return self._width

    def get_height(self):
        """Gets height
        """
        return self._height

    def get_file_path(self):
        """Gets path to image
        """
        return self._path

    def get_format(self):
        """
        :return:
        """
        return self._format

    def get_size_in_bytes(self):
        """ Gets size in bytes
        :return: size in bytes
        """
        return self._size_in_bytes


class ImageStatsFromDirectoryFactory(object):
    """Creates ImageStats objects from directory of images
    """

    def __init__(self, directory, max_image_pixels=768000000):
        """Constructor
        """
        self._directory = directory
        logger.debug('Setting MAX_IMAGE_PIXELS to ' + str(max_image_pixels))
        Image.MAX_IMAGE_PIXELS = max_image_pixels

    def get_input_image_stats(self,
                              keysortfunc=None):
        """Gets InputImageStats objects as list
        """
        image_stats_list = []
        if os.path.isfile(self._directory):
            return []
        file_list = get_image_path_list(self._directory,
                                        None)
        for fp in file_list:
            im = None
            try:
                im = Image.open(fp)
                iis = ImageStats(fp, im.size[0],
                                 im.size[1], im.format,
                                 size_in_bytes=os.path.getsize(fp))
                image_stats_list.append(iis)
            except Exception:
                logger.exception('Skipping file unable to open ' + fp)
            finally:
                try:
                    im.close()
                except Exception:
                    logger.exception('Caught exception attempting '
                                     'to close image')

        return image_stats_list


class ImageStatsSummary(object):
    """Represents summary information for multiple ImageStats
       objects
    """
    def __init__(self):
        """Constructor
        """
        self._num_images = 0
        self._total_size = 0
        self._total_pixels = 0
        self._dimension_hash = {}

    def add_image_stats(self, image_stats):
        """Adds `ImageStats` (image_stats) to this summary
        :param image_stats: ImageStats object
        """
        if image_stats is None:
            logger.warning('None passed into add_image_stats')
            return

        self._num_images += 1
        self._total_size += image_stats.get_size_in_bytes()
        dim_tuple = (image_stats.get_width(), image_stats.get_height())
        if dim_tuple in self._dimension_hash.keys():
            self._dimension_hash[dim_tuple] += 1
        else:
            self._dimension_hash[dim_tuple] = 1
        self._total_pixels += dim_tuple[0] * dim_tuple[1]

    def get_image_count(self):
        """Returns number of images
        :returns: total number of images
        """
        return self._num_images

    def get_total_size_of_images_in_bytes(self):
        """Returns total size of images in bytes
        :returns: total size of all images in bytes
        """
        return self._total_size

    def get_image_dimensions_as_dict(self):
        """Gets dictionary of image dimensions.
        :returns: dictionary of image dimensions where key is
                  tuple (W,H) and value is number of images with
                  given image dimension
        """
        return self._dimension_hash

    def get_total_pixels(self):
        """Gets total number of pixels in images
        :returns: total number of pixels ie W x H x # images
        """
        return self._total_pixels

