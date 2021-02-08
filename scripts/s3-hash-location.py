#! /usr/bin/env python3

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
    # fake data for testing
    salt = "BFb7aJ5GR5TDmrOrZpfWh44kIGnS9QN2"
    ordinal = "0"
    subscriber = "sub.global"
    receiver = "email1@example.com,email2@example.com"
    uniq = hash_key(salt, ordinal, subscriber, receiver)
    print(uniq)
