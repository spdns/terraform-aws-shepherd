#! /usr/bin/env python3

import argparse
import hashlib


def hash_key(salt, ordinal, subscriber, receiver):
    salt_bytes = bytes(salt, "utf-8")
    ordinal_bytes = bytes(ordinal, "utf-8")
    subscriber_bytes = bytes(subscriber, "utf-8")
    receiver_bytes = bytes(receiver, "utf-8")
    dk = hashlib.pbkdf2_hmac(
        "sha512", subscriber_bytes + receiver_bytes, salt_bytes + ordinal_bytes, 100000
    )
    return dk.hex()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("--salt", help="salt value")
    parser.add_argument("--ordinal", type=int, help="ordinal value")
    parser.add_argument("--subscriber", help="subscriber value")
    parser.add_argument("--receiver", help="receiver value")

    args = parser.parse_args()
    uniq = hash_key(args.salt, str(args.ordinal), args.subscriber, args.receiver)
    print(uniq)
