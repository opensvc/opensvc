# pylint: skip-file

"""
/* MD5C.C - RSA Data Security, Inc., MD5 message-digest algorithm
 */

/* Copyright (C) 1991-2, RSA Data Security, Inc. Created 1991. All
rights reserved.

License to copy and use this software is granted provided that it
is identified as the "RSA Data Security, Inc. MD5 Message-Digest
Algorithm" in all material mentioning or referencing this software
or this function.

License is also granted to make and use derivative works provided
that such works are identified as "derived from the RSA Data
Security, Inc. MD5 Message-Digest Algorithm" in all material
mentioning or referencing the derived work.

RSA Data Security, Inc. makes no representations concerning either
the merchantability of this software or the suitability of this
software for any particular purpose. It is provided "as is"
without express or implied warranty of any kind.

These notices must be retained in any copies of any part of this
documentation and/or software.
 */
"""

#/* Constants for MD5Transform routine.

S11 = 7
S12 = 12
S13 = 17
S14 = 22
S21 = 5
S22 = 9
S23 = 14
S24 = 20
S31 = 4
S32 = 11
S33 = 16
S34 = 23
S41 = 6
S42 = 10
S43 = 15
S44 = 21

PADDING = "\x80" + 63*"\0"   # do not overlook first byte again :-)

#/* F, G, H and I are basic MD5 functions.
def F(x, y, z): return (((x) & (y)) | ((~x) & (z)))

def G(x, y, z): return (((x) & (z)) | ((y) & (~z)))

def H(x, y, z): return ((x) ^ (y) ^ (z))

def I(x, y, z): return((y) ^ ((x) | (~z)))

#/* ROTATE_LEFT rotates x left n bits.

def ROTATE_LEFT(x, n):
    x = x & 0xffffffff   # make shift unsigned
    return (((x) << (n)) | ((x) >> (32-(n)))) & 0xffffffff

#/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4.
#Rotation is separate from addition to prevent recomputation.

def FF(a, b, c, d, x, s, ac):
    a = a + F ((b), (c), (d)) + (x) + (ac)
    a = ROTATE_LEFT ((a), (s))
    a = a + b
    return a # must assign this to a

def GG(a, b, c, d, x, s, ac):
    a = a + G ((b), (c), (d)) + (x) + (ac)
    a = ROTATE_LEFT ((a), (s))
    a = a + b
    return a # must assign this to a

def HH(a, b, c, d, x, s, ac):
    a = a + H ((b), (c), (d)) + (x) + (ac)
    a = ROTATE_LEFT ((a), (s))
    a = a + b
    return a # must assign this to a

def II(a, b, c, d, x, s, ac):
    a = a + I ((b), (c), (d)) + (x) + (ac)
    a = ROTATE_LEFT ((a), (s))
    a = a + b
    return a # must assign this to a


class md5:
    def __init__(self, initial=None):
        self.count = 0
        self.state = (0x67452301,
                      0xefcdab89,
                      0x98badcfe,
                      0x10325476,)
        self.buffer = ""
        if initial:
            self.update(initial)

##/* MD5 block update operation. Continues an MD5 message-digest
##  operation, processing another message block, and updating the
##  context.
## */

##  /* Compute number of bytes mod 64 */
    def update(self, input):
        inputLen = len(input)
##  index = (unsigned int)((context->count[0] >> 3) & 0x3F);
        index = int(self.count >> 3) & 0x3F

##  /* Update number of bits */
        self.count = self.count + (inputLen << 3)

##  partLen = 64 - index;
        partLen = 64 - index

##  /* Transform as many times as possible.
        if inputLen >= partLen:
            self.buffer = self.buffer[:index] + input[:partLen]
            self.transform(self.buffer)
            i = partLen
            while i + 63 < inputLen:
                self.transform(input[i:i+64])
                i = i + 64
            index = 0
        else:
            i = 0

##  /* Buffer remaining input */
        self.buffer = self.buffer[:index] + input[i:inputLen]


##/* MD5 finalization. Ends an MD5 message-digest operation, writing the
##  the message digest and zeroizing the context.
## */

    def final(self):

##  /* Save number of bits */
        bits = Encode((self.count & 0xffffffff, self.count>>32), 8)

##  /* Pad out to 56 mod 64.

        index = int((self.count >> 3) & 0x3f)
        if index < 56:
            padLen = (56 - index)
        else:
            padLen = (120 - index)
        self.update(PADDING[:padLen])

##  /* Append length (before padding) */
        self.update(bits)

##  /* Store state in digest */
        digest = Encode(self.state, 16)

##  /* Zeroize sensitive information.

        self.__dict__.clear()

        return digest

    digest = final  # alias

##/* MD5 basic transformation. Transforms state based on block.
## */

    def transform(self, block):
        a, b, c, d = state = self.state

        x = Decode(block, 64)

##  /* Round 1 */
        a = FF (a, b, c, d, x[ 0], S11, 0xd76aa478)#; /* 1 */
        d = FF (d, a, b, c, x[ 1], S12, 0xe8c7b756)#; /* 2 */
        c = FF (c, d, a, b, x[ 2], S13, 0x242070db)#; /* 3 */
        b = FF (b, c, d, a, x[ 3], S14, 0xc1bdceee)#; /* 4 */
        a = FF (a, b, c, d, x[ 4], S11, 0xf57c0faf)#; /* 5 */
        d = FF (d, a, b, c, x[ 5], S12, 0x4787c62a)#; /* 6 */
        c = FF (c, d, a, b, x[ 6], S13, 0xa8304613)#; /* 7 */
        b = FF (b, c, d, a, x[ 7], S14, 0xfd469501)#; /* 8 */
        a = FF (a, b, c, d, x[ 8], S11, 0x698098d8)#; /* 9 */
        d = FF (d, a, b, c, x[ 9], S12, 0x8b44f7af)#; /* 10 */
        c = FF (c, d, a, b, x[10], S13, 0xffff5bb1)#; /* 11 */
        b = FF (b, c, d, a, x[11], S14, 0x895cd7be)#; /* 12 */
        a = FF (a, b, c, d, x[12], S11, 0x6b901122)#; /* 13 */
        d = FF (d, a, b, c, x[13], S12, 0xfd987193)#; /* 14 */
        c = FF (c, d, a, b, x[14], S13, 0xa679438e)#; /* 15 */
        b = FF (b, c, d, a, x[15], S14, 0x49b40821)#; /* 16 */

## /* Round 2 */
        a = GG (a, b, c, d, x[ 1], S21, 0xf61e2562)#; /* 17 */
        d = GG (d, a, b, c, x[ 6], S22, 0xc040b340)#; /* 18 */
        c = GG (c, d, a, b, x[11], S23, 0x265e5a51)#; /* 19 */
        b = GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa)#; /* 20 */
        a = GG (a, b, c, d, x[ 5], S21, 0xd62f105d)#; /* 21 */
        d = GG (d, a, b, c, x[10], S22,  0x2441453)#; /* 22 */
        c = GG (c, d, a, b, x[15], S23, 0xd8a1e681)#; /* 23 */
        b = GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8)#; /* 24 */
        a = GG (a, b, c, d, x[ 9], S21, 0x21e1cde6)#; /* 25 */
        d = GG (d, a, b, c, x[14], S22, 0xc33707d6)#; /* 26 */
        c = GG (c, d, a, b, x[ 3], S23, 0xf4d50d87)#; /* 27 */
        b = GG (b, c, d, a, x[ 8], S24, 0x455a14ed)#; /* 28 */
        a = GG (a, b, c, d, x[13], S21, 0xa9e3e905)#; /* 29 */
        d = GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8)#; /* 30 */
        c = GG (c, d, a, b, x[ 7], S23, 0x676f02d9)#; /* 31 */
        b = GG (b, c, d, a, x[12], S24, 0x8d2a4c8a)#; /* 32 */

##  /* Round 3 */
        a = HH (a, b, c, d, x[ 5], S31, 0xfffa3942)#; /* 33 */
        d = HH (d, a, b, c, x[ 8], S32, 0x8771f681)#; /* 34 */
        c = HH (c, d, a, b, x[11], S33, 0x6d9d6122)#; /* 35 */
        b = HH (b, c, d, a, x[14], S34, 0xfde5380c)#; /* 36 */
        a = HH (a, b, c, d, x[ 1], S31, 0xa4beea44)#; /* 37 */
        d = HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9)#; /* 38 */
        c = HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60)#; /* 39 */
        b = HH (b, c, d, a, x[10], S34, 0xbebfbc70)#; /* 40 */
        a = HH (a, b, c, d, x[13], S31, 0x289b7ec6)#; /* 41 */
        d = HH (d, a, b, c, x[ 0], S32, 0xeaa127fa)#; /* 42 */
        c = HH (c, d, a, b, x[ 3], S33, 0xd4ef3085)#; /* 43 */
        b = HH (b, c, d, a, x[ 6], S34,  0x4881d05)#; /* 44 */
        a = HH (a, b, c, d, x[ 9], S31, 0xd9d4d039)#; /* 45 */
        d = HH (d, a, b, c, x[12], S32, 0xe6db99e5)#; /* 46 */
        c = HH (c, d, a, b, x[15], S33, 0x1fa27cf8)#; /* 47 */
        b = HH (b, c, d, a, x[ 2], S34, 0xc4ac5665)#; /* 48 */

##  /* Round 4 */
        a = II (a, b, c, d, x[ 0], S41, 0xf4292244)#; /* 49 */
        d = II (d, a, b, c, x[ 7], S42, 0x432aff97)#; /* 50 */
        c = II (c, d, a, b, x[14], S43, 0xab9423a7)#; /* 51 */
        b = II (b, c, d, a, x[ 5], S44, 0xfc93a039)#; /* 52 */
        a = II (a, b, c, d, x[12], S41, 0x655b59c3)#; /* 53 */
        d = II (d, a, b, c, x[ 3], S42, 0x8f0ccc92)#; /* 54 */
        c = II (c, d, a, b, x[10], S43, 0xffeff47d)#; /* 55 */
        b = II (b, c, d, a, x[ 1], S44, 0x85845dd1)#; /* 56 */
        a = II (a, b, c, d, x[ 8], S41, 0x6fa87e4f)#; /* 57 */
        d = II (d, a, b, c, x[15], S42, 0xfe2ce6e0)#; /* 58 */
        c = II (c, d, a, b, x[ 6], S43, 0xa3014314)#; /* 59 */
        b = II (b, c, d, a, x[13], S44, 0x4e0811a1)#; /* 60 */
        a = II (a, b, c, d, x[ 4], S41, 0xf7537e82)#; /* 61 */
        d = II (d, a, b, c, x[11], S42, 0xbd3af235)#; /* 62 */
        c = II (c, d, a, b, x[ 2], S43, 0x2ad7d2bb)#; /* 63 */
        b = II (b, c, d, a, x[ 9], S44, 0xeb86d391)#; /* 64 */

        self.state = (0xffffffff & (state[0] + a),
                      0xffffffff & (state[1] + b),
                      0xffffffff & (state[2] + c),
                      0xffffffff & (state[3] + d),)

##  /* Zeroize sensitive information.

        del x

# end of the class. Now the helpers

import struct, string

def Encode(input, len):
    k = len >> 2
    res = apply(struct.pack, ("%iI" % k,) + tuple(input[:k]))
    return string.join(res, "")

def Decode(input, len):
    k = len >> 2
    res = struct.unpack("%iI" % k, input[:len])
    return list(res)

def test():
    print(repr(md5("/dev/sda").digest().encode('hex')))
    #from md5 import new
    #print(`new("/dev/sda").digest()`)

if __name__=="__main__":
    test()
