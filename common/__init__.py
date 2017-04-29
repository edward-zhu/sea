def _fnv32a(s):
    hval = 0x811c9dc5
    fnv_32_prime = 0x01000193
    uint32_max = 2 ** 32
    for c in s:
        hval = hval ^ ord(c)
        hval = (hval * fnv_32_prime) % uint32_max
    return hval

def hashf(s):
    return _fnv32a(s)