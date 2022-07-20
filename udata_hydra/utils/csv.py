import csv
from cchardet import UniversalDetector


def find_delimiter(filename):
    """Find delimiter for eventual csv file"""
    sniffer = csv.Sniffer()
    with open(filename) as fp:
        delimiter = sniffer.sniff(fp.read(5000), delimiters=';,|\t').delimiter
    return delimiter

def detect_encoding(the_file):
    '''Detects file encoding using chardet based on N first lines
    '''
    detector = UniversalDetector()
    for line in the_file.readlines():
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return detector.result['encoding']
