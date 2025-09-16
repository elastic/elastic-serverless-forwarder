
from io import BytesIO
from share import ipfix_parser

try:
    with open('data.ipfix', 'rb') as f:
        for record in ipfix_parser.parse_ipfix_stream(f):
            print(f"Record: {record}")
except FileNotFoundError:
    print(f"no file found!")

except Exception as e:
    print(f"some other problem: {e}")
