import concurrent.futures
from pprint import pprint
from collections import  Counter

some_primes = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53,
               59,61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113,
               127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181,
               191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251,
               257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317,
               331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397,
               401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463,
               467, 479, 487, 491, 499, 503, 509, 521, 523, 541}


def miller_rabin_pass(a: int, s: int, d: int, n: int) -> bool:
    a_to_power = pow(a, d, n)
    if a_to_power == 1:
        return True

    for _ in range(s-1):
        if a_to_power == n - 1:
            return True
        a_to_power = (a_to_power * a_to_power) % n
    return a_to_power == n - 1


def primality_test(n: int) -> bool:
    '''
    For miller rabin
    if n < 1,373,653 it is enough to test a = 2 and 3;
    if n < 9,080,191 it is enough to test a = 31 and 73;
    if n < 4,759,123,141 it is enough to test a = 2, 7, and 61;
    if n < 2,152,302,898,747 it is enough to test a = 2, 3, 5, 7, and 11
    '''
    if n < 2:
        return False

    if n in some_primes:
        return True

    if n < 1_373_653:
        tests = (2, 3)
    elif n < 9_080_191:
        tests = (31, 73)
    elif n < 4_759_123_141:
        tests = (2, 7, 61)
    elif n < 2_152_302_898_747:
        tests = (2, 3, 5, 7, 11)
    else:
        raise ValueError("I can't be bothered writting a fully"
                         " deterministic miller_rabin test")

    d = n - 1
    s = 0
    while d % 2 == 0:
        d >>= 1
        s += 1 

    for a in tests:
        if not miller_rabin_pass(a, s, d, n):
            return False
    return True


def count_prime_gaps_per_chunk(chunk_start: int, chunk_end: int) -> Counter:
    """
    When we split our search in to chunks we have to make sure we're not
    missing the prime gaps that occur over the boundry of said chunks.

    We do that by searching for the previous prime before the chunk starts.
    That will our first "previous prime".
    """
    # In general we are only search on odd numbers
    if chunk_start % 2 == 0:
        chunk_start += 1
    
    # Special case for 3 as there are no odd primes smaller than 3
    if chunk_start == 3:
        chunk_start = 5

    # Find Previous Prime First
    for candidate in range(chunk_start - 2, 1, -1):
        if primality_test(candidate):
            prev_prime = candidate
            break
    else:
        raise ValueError("You did something wrong")

    # Start counting the gaps!
    gap_counter = Counter()
    for candidate in range(chunk_start, chunk_end, 2):
        if primality_test(candidate):
            gap_counter[candidate - prev_prime] += 1
            prev_prime = candidate
    
    return gap_counter


def main():
    # Number of chunks should be at least 2x the number of cores on your CPU
    number_of_chunks = 101

    # Maximum values should be MUCH larger than number of chunks
    maximum_value = 150_000_000

    # When we spawn a worker to go and search prime gaps this gives
    # us a "future" object, which is a sort of place holder.
    #
    # The "future" object will in the future give us the result by
    # calling the "result()" method on it.
    #
    # How do we know when it's ready? The "with" statement handles
    # that for us, it will wait until it completes, so after the "with"
    # statement we know it's ready
    futures = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i in range(number_of_chunks):
            start_of_chunk = max(3, (maximum_value * i)//number_of_chunks)
            end_of_chunk = min(maximum_value, (maximum_value * (i + 1))//number_of_chunks)
            futures.append(
                executor.submit(count_prime_gaps_per_chunk, start_of_chunk, end_of_chunk)
                )
    
    # The Counter objects we get back are very nicely summable
    gap_counter = sum((future.result() for future in futures), Counter())
    pprint(gap_counter.most_common())


if __name__ == '__main__':
    main()
