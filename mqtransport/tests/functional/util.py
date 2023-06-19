from .settings import load_app_settings

SLEEP_TIME_SHORT = 1
SLEEP_TIME_LONG = 5


def mq_is_auth_disabled() -> bool:
    settings = load_app_settings()
    return settings.message_queue.broker == "sqs"


def unordered_unique_match(left: list, right: list):
    set_left = set(left)
    set_right = set(right)
    no_duplicates_left = len(left) == len(set_left)
    no_duplicates_right = len(right) == len(set_right)
    return set_left == set_right and no_duplicates_left and no_duplicates_right
