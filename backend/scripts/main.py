
import backend
import json

filename = "data.ipfix"

proc = backend.IpfixProcessor(filename)

while proc.has_more():
    value = proc.next()
    print(value)
