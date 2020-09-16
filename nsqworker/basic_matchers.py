import json
import re

import mdict


def predicate_field_matcher(field, missing_value=False, inverse=False):

    def match(message):

        try:
            msg = json.loads(message)
        except ValueError:
            return False

        md = mdict.MDict(msg)
        val = bool(md.get(field, default=missing_value))

        return val if not inverse else not val

    return match


def json_matcher(field, value):
    """Basic JSON matcher

    Returns a method for matching a message `field` against `value`
    """
    def match(message):
        try:
            message = json.loads(message)
        except ValueError:
            return False

        return message.get(field) == value

    return match


def json_mdict_matcher(field, value):
    """Basic JSON->Dict matcher

    Returns a method for matching a message `field:internalField:internalInternalField` against `value`
    """
    def match(message):
        try:
            message = json.loads(message)
            md = mdict.MDict(message)
        except ValueError:
            return False

        return md.get(field) == value

    return match


def regex_matcher(pattern):
    """Basic regular expression matcher

    Returns a method for matching a message against a given regex pattern
    """
    def match(message):
        return re.match(pattern, message) is not None

    return match


def multi_matcher(*matcher_funcs):

    def match(message):
        for mf in matcher_funcs:
            if not mf(message):
                return False

        return True

    return match
