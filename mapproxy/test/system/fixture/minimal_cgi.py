#! /usr/bin/env python3

"""
CGI script that returns a red 256x256 PNG file.
"""

if __name__ == '__main__':
    import sys
    if sys.version_info[0] == 2:
        w = sys.stdout.write
    else:
        w = sys.stdout.buffer.write
    w(b"Content-type: image/png\r\n")
    w(b"\r\n")

    w(b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x01\x00\x00\x00\x01\x00\x01\x03\x00\x00\x00f\xbc:%\x00\x00\x00\x06PLTE\xff\x00\x00\x00\x00\x00A\xa3\x12\x03\x00\x00\x00\x1fIDATx\x9c\xed\xc1\x01\r\x00\x00\x00\xc2\xa0\xf7Om\x0e7\xa0\x00\x00\x00\x00\x00\x00\x00\x00\xbe\r!\x00\x00\x01\xf1g!\xee\x00\x00\x00\x00IEND\xaeB`\x82')