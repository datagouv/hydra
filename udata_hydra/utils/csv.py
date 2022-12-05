import csv
from cchardet import UniversalDetector


def find_delimiter(filename, encoding="utf-8"):
    """Find delimiter for eventual csv file"""
    delimiter = None
    sniffer = csv.Sniffer()
    try:
        with open(filename, encoding=encoding) as fp:
            delimiter = sniffer.sniff(fp.read(5000), delimiters=";,|\t").delimiter
    # unknown encoding, utf-8 but not, sniffer failed
    except (LookupError, UnicodeDecodeError, csv.Error):
        pass
    return delimiter


def detect_encoding(filename):
    """Detects file encoding using cchardet"""
    detector = UniversalDetector()
    with open(filename, mode="rb") as f:
        for line in f.readlines():
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    return detector.result.get("encoding")
