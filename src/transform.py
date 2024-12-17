from datetime import datetime
from typing import Optional, List, Dict, Text, Any, Callable, Union

import dateparser
import pytz


def _adjust_timezone(x: datetime) -> datetime:
    """
    Converts datetime to UTC Europe/Oslo datetime.

    Args:
        x (datetime): datetime value to be augmented with timezone info
    Returns:
        datetime: datetime with timezone information UTC Europe/Oslo added or replaced
    """
    timezone = pytz.timezone("Europe/Oslo")
    if x.tzinfo is not None:
        x = (x - x.utcoffset()).replace(tzinfo=None)  # type: ignore
    x += timezone.utcoffset(x, is_dst=True)  # type: ignore
    return x


def identity(x: Any) -> Any:
    """
    Identity function.

    Default for unspecified fun.
    """
    return x


def str_to_code(x: Optional[Text]) -> Text:
    """
    fun: str -> str-code

    A function converting a string to a code
    conforming to DVH-utviklingsstandard 2.3.
    """
    if x is None:
        return "UKJENT"

    y = "_".join(x.split()).upper().replace("Æ", "A").replace("Ø", "O").replace("Å", "AA")
    y = "".join(filter(lambda x: x in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_", y))
    y = "UKJENT" if y == "" else y
    return y


def str_to_date(x: Optional[Text]) -> Optional[datetime]:
    """
    fun: str -> datetime-no

    Converts string to Europe/Oslo datetime.
    """
    if x is None:
        return None
    y = dateparser.parse(x, languages=["en"])  # languages=["nn", "nb"])
    if y is None:
        return None
    return _adjust_timezone(y)


def int_s_to_date(x: Optional[Union[int, float]]) -> Optional[datetime]:
    """
    fun: int-unix-s -> datetime-no

    Converts integer representing
    seconds epoch to Europe/Oslo datetime.
    """
    if x is None:
        return None
    y = datetime.utcfromtimestamp(x)
    return _adjust_timezone(y)


def int_ms_to_date(x: Optional[int]) -> Optional[datetime]:
    """
    fun: int-unix-ms -> datetime-no

    Converts integer representing
    milliseconds epoch to Europe/Oslo datetime
    keeping milliseconds precision
    """
    if x is None:
        return None
    return int_s_to_date(x / 1000)


def bool_to_int(x: Optional[bool]) -> Optional[int]:
    """
    fun: bool -> int

    Converts integer to boolean.
    """
    if x is None:
        return None
    return int(x)


def datetime_to_datetime_no(x: Optional[datetime]) -> Optional[datetime]:
    """
    fun: datetime -> datetime-no

    Converts datetime to Europe/Oslo datetime.
    """
    if x is None:
        return None
    return _adjust_timezone(x)


TRANSFORMS = {
    "str -> str-code": str_to_code,
    "str -> datetime-no": str_to_date,
    "int-unix-s -> datetime-no": int_s_to_date,
    "int-unix-ms -> datetime-no": int_ms_to_date,
    "bool -> int": bool_to_int,
    "datetime -> datetime-no": datetime_to_datetime_no,
}


class Transform:
    """
    A class encapsulating message column data transform.

    Args:
        transforms (list)

    Attributes:
        extract (dict):
        cast (dict)
    """

    class Rule:
        """
        Internal class for transform lookups
        """

        def __init__(self, tconf: Dict[Text, Any]) -> None:
            self.src: Text = tconf["src"]
            self.dst: Text = tconf["dst"]
            self.fun: Callable = TRANSFORMS.get(tconf.get("fun", None), identity)
            self.allow_undefined: bool = tconf.get("allow_undefined", False)

        def cast(self, item: Any):
            return self.fun(item)

    def __init__(self, transforms: List[Dict[Text, Any]]) -> None:
        self.rules: List[Transform.Rule] = [Transform.Rule(transform) for transform in transforms]

    def __call__(self, message: Dict[Text, Any]) -> Dict[Text, Any]:
        """Returns transformed message

        Args:
            message (dict): kafka-message represented as a dictionary
        Returns:
            transformed_message (dict): kafka-message as a dictionary augmented with (optional) transformations
        """

        transformed_message = dict()
        batch_time = datetime.today()
        ## TODO lokalt er det $$BATCH_TIME, i airflow brukes $$$BATCH_TIME
        for rule in self.rules:
            if rule.src[:2] == "$$":
                if rule.src[2:] == "BATCH_TIME":
                    item = batch_time
                else:
                    raise NotImplementedError("unsupported identifier")
            elif rule.src[:1] == "$":
                item = rule.src[1:]  # string literal
            else:
                sub_paths = rule.src.split(".")
                try:
                    item = message[sub_paths.pop(0)]
                    while len(sub_paths) > 0:
                        item = item[sub_paths.pop(0)]
                except KeyError:
                    if rule.allow_undefined:
                        item = None
                    else:
                        raise KeyError(f"path `{rule.src}` is undefined") from None
                try:
                    item = rule.cast(item)  # f:src->dst
                except Exception as e:
                    item_type = type(item)
                    raise e.__class__(
                        f"when calling {rule.fun.__name__}() with `{rule.src}` of type `{item_type.__name__}`"
                    ) from None

            transformed_message[rule.dst] = item  # {dst: y=f(x)}
        return transformed_message
