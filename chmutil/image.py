# -*- coding: utf-8 -*-

import logging
from PIL import Image

logger = logging.getLogger(__name__)


class InvalidImageError(Exception):
    """Denotes invalid image object
    """
    pass


class ImageThresholder(object):
    """Thresholds image by percent specified
    """

    def __init__(self, threshold_percent=30):
        """
        Constructor
        :param threshold_percent: int value ranging between 0 and 100 where
                                  0 is 0% and 100 is 100%
        """
        self._threshold_percent = threshold_percent
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
