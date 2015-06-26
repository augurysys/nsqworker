import json
import re


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


def regex_matcher(pattern):
    """Basic regular expression matcher

    Returns a method for matching a message against a given regex pattern
    """
    def match(message):
        return re.match(pattern, message) is not None

    return match
