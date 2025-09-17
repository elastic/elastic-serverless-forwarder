
import backend

print(backend.sum_as_string(23, 45))

filename = "data.ipfix"

proc = backend.IpfixProcessor(filename)
print(f"opening the file: {proc.open()}")

while proc.has_more():
    print(f"went through at least once")
    val = proc.next()
    print(f"value is {val}")
    break
