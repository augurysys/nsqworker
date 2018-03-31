import random
from string import hexdigits

random.seed()

def gen_random_string(n=10):
    return ''.join(random.choice(hexdigits) for _ in range(n))