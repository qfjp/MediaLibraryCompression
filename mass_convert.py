#!/bin/python
import traceback
import argparse as ap
import datetime
import grp
import json
import math
import os
import pathlib as p
import pwd
import re
import shutil
import signal
import subprocess as s
import sys
import types
from enum import Enum, auto, unique
from itertools import chain
from textwrap import TextWrapper
from typing import (
    Any,
    Callable,
    Iterable,
    Literal,
    Optional,
    ParamSpec,
    TextIO,
    TypedDict,
    TypeVar,
    overload,
)

import ffpb  # type: ignore[import-untyped]
import requests
from secrets import phone_number
from overrides import overrides

os.nice(10)

TURN_OFF_MAIN = False
# For when we quit unexpectedly
TOTAL_SAVED = 0
TIME_START = datetime.datetime.now()
CURRENT_CONVERT = p.Path("")
CURRENT_OBJECT = p.Path("")
VERIFIED_TRANSCODES = []

@unique
class VidContainer(Enum):
    Matroska = auto()
    MPEG_4 = auto()
    _valid_exts = {MPEG_4: set([".mp4", ".m4v"]), Matroska: set([".mkv"])}
    _output_exts = {MPEG_4: ".mp4", Matroska: ".mkv"}
    _medianfo_container_names = {MPEG_4: "MPEG-4", Matroska: "Matroska"}

    @classmethod
    def _missing_(cls, name):  # type: ignore[misc, no-untyped-def]
        name = name.lower().replace("-", "_")
        if name == "matroska":
            return cls.Matroska
        if name == "mpeg4" or name == "mpeg-4":
            return cls.MPEG_4

    def valid_exts(self) -> set[str]:
        return self._valid_exts.value[self.value]  # type: ignore[attr-defined, no-any-return]

    def output_ext(self) -> str:
        return self._output_exts.value[self.value]  # type: ignore[attr-defined, no-any-return]

    def json_name(self) -> str:
        return self._medianfo_container_names.value[self.value]  # type: ignore[attr-defined, no-any-return]

    def __str__(self) -> str:
        return self.name


class CliState:
    """
    Stub class to hold the parsed arguments.

    This is only necessary for type checking, ultimately the following
    would also work:

    .. code:: python

        class CliState:
            pass

    """

    FILES: list[p.Path]
    user: str
    uid: int
    group: str
    gid: int
    verbosity: int
    umask: int
    list_files: bool
    exact: bool
    no_color: bool
    keep_failures: bool
    keep_original: bool
    dry_run: bool
    num_files: int
    encoder: str
    container: VidContainer


@unique
class Tern(Enum):
    TFalse = False
    TTrue = True
    TOther = auto()

    def __cls__(cls, value: object) -> "Tern":
        if value == Tern.TOther:
            return Tern.TOther
        return Tern.from_bool(bool(value))

    def __bool__(self) -> bool:
        return bool(self.value)

    @staticmethod
    def from_bool(b: bool) -> "Tern":
        if b:
            return Tern.TTrue
        else:
            return Tern.TFalse


class UMask:
    def __init__(self, string: str) -> None:
        """
        :param string: An octal umask (string of 3 or 4 numbers, from 0 to 7).
        """
        if len(string) not in [3, 4]:
            raise ValueError("UMask must be a string of 3 or 4 numbers")
        overall_mask = 0
        for ix, digit_str in enumerate(string[:0:-1]):
            digit = int(digit_str)
            if digit not in list(range(8)):
                raise ValueError("UMask digits must match [0-7]")
            statmask = 7 - int(digit)
            val = statmask << (3 * ix)
            overall_mask |= val
        special_val = 0
        if len(string) == 4:
            special_val = int(string[0])
        overall_mask |= special_val << (3 * 3)
        self.mask = overall_mask


@unique
class StreamType(Enum):
    """
    The type of a stream that can be found in a video container
    file. These are the possible values of "@type" in the objects
    returned by :code:`mediainfo --output=JSON ...`
    """

    General = "General"
    Video = "Video"
    Audio = "Audio"
    Text = "Text"
    Menu = "Menu"

    def max_streams(self) -> Optional[int]:
        """
        The maximum number of streams of a given type that `should` be
        encountered in a typical output of :func:`mediainfo`. This is
        used as a sanity check.
        """
        if self == StreamType.General:
            return 1
        else:
            return None

    def ffprobe_ident(self) -> str:
        """
        The identifier for a given stream type used by :code:`ffprobe`.
        """
        if self == StreamType.Text:
            return "s"
        return self.name[0].lower()


# NVENC_SETTINGS = ["-tune", "hq", "-rc", "vbr", "-multipass", "qres", "-b_ref_mode", "each", "-rc-lookahead", 32, "-strict_gop", "1", "-spatial_aq", "1", "temporal_aq", "1"]
# AV1_ENCODER = "libsvtav1"

# Extensions of convertable files
VIDEO_FILE_EXTS = set([".avi", ".mpg", ".mkv", ".m2ts", ".webm", ".m4v", ".mp4"])
# OLD OLD Extensions, questionable quality
QUESTIONABLE_EXTS = set([".avi", ".mpg"])

COLLISION_SUFFIX = "converted"
PRINT_WIDTH = shutil.get_terminal_size().columns
CLI_STATE = CliState()
DEBUG_STREAM = sys.stdout
D_STRING = "--> "

# Preserve AV1/VP9 encoded videos for now
SKIP_CODECS = ["AV1", "VP9"]
# Leave video codecs as None to be replaced by the argument parser
CODEC_ID_MAP = {
    # Video
    # "V_AV1": AV1_ENCODER,
    "V_MPEG2": None,
    "V_MPEG4/ISO/AVC": None,
    "V_MPEGH/ISO/HEVC": None,
    "avc1": None,
    "V_MS/VFW/FOURCC / WVC1": None,
    "XVID": None,
    "DX50": None,  # DivX
    "27": None,  # Bluray for "x264"
    "mp4v-20": None,
    # Audio
    "A_EAC3": "copy",
    "ec-3": "copy",
    "A_DTS": "copy",
    "A_AC3": "copy",
    "ac-3": "copy",
    "2000": "copy",  # Found for AC-3 in an avi/xvid file
    "131": "copy",  # Bluray for AC-3
    "129": "copy",  # Bluray for AC-3
    "55": "copy",  # AVI for MPEG Audio
    "A_TRUEHD": "copy",
    "AAC": "copy",
    "A_AAC-2": "copy",
    "mp4a-40-2": "copy",
    "A_MPEG/L2": "copy",
    "A_MPEG/L3": "copy",
    "A_OPUS": "copy",
    "A_VORBIS": "copy",
    "A_FLAC": "copy",
    "A_PCM/INT/LIT": "copy",
    # Subtitle
    "S_DVBSUB": "dvdsub",
    "S_HDMV/PGS": "dvdsub",
    "144": "dvdsub",  # Bluray for PGS
    "S_VOBSUB": "dvdsub",
    "mp4s-E0": "dvdsub",
    "S_TEXT/ASS": "mov_text",
    "S_TEXT/UTF8": "mov_text",
    "tx3g": "mov_text",
    # Analog Subtitle
    "c608": "copy",
}


def true_if_left_empty(func: Callable[[str, str], bool]) -> Callable[[str, str], bool]:
    """
    Wraps a function taking two arguments, and returns True if the left
    argument is None.

    This is used with the metadata comparison functions. If the
    original does not have a value (the left argument) then the test
    should automatically pass.
    """

    def inner(str1: str, str2: str) -> bool:
        if str1 == "":
            return True
        return func(str1, str2)

    return inner


def truthy_if_right_empty(
    func: Callable[[str, str], bool]
) -> Callable[[str, str], Tern]:
    def inner(str1: str, str2: str) -> Tern:
        if str2 == "":
            return Tern.TOther
        return Tern.from_bool(func(str1, str2))

    return inner


def never_false(func: Callable[[str, str], bool]) -> Callable[[str, str], Tern]:
    """
    Convert a boolean function to a never-false ternary function.
    :return: λ(s1, s2) = 1 + func(s1, s2)
    """

    def inner(str1: str, str2: str) -> Tern:
        result = func(str1, str2)
        if not result:
            return Tern.TOther
        return Tern.TTrue

    return inner


def safer_float(string: str) -> float:
    try:
        return float(string)
    except ValueError:
        return 0


def safer_int(string: str) -> int:
    try:
        return int(string)
    except ValueError:
        return 0


@true_if_left_empty
def exact_match(x: str, y: str) -> bool:
    return x == y


@true_if_left_empty
def exact_int(x: str, y: str) -> bool:
    return safer_int(x) == safer_int(y)


@true_if_left_empty
def set_match(x: str, y: str) -> bool:
    return string_val_to_set(x) == string_val_to_set(y)


@true_if_left_empty
def almost_int(x: str, y: str) -> bool:
    return math.fabs(safer_int(x) - safer_int(y)) <= 1


@true_if_left_empty
def fuzzy_int(x: str, y: str) -> bool:
    return math.fabs(safer_int(x) - safer_int(y)) <= INT_TOLERANCE


@true_if_left_empty
def fuzzy_float(x: str, y: str) -> bool:
    return math.fabs(safer_float(x) - safer_float(y)) <= FLOAT_TOLERANCE


@true_if_left_empty
def fuzziest_float(x: str, y: str) -> bool:
    return math.fabs(safer_float(x) - safer_float(y)) <= FUZZIEST_TOLERANCE


@true_if_left_empty
def new_is_more(x: str, y: str) -> bool:
    return safer_int(x) <= safer_int(y)


def new_is_less(x: str, y: str) -> bool:
    return int(x) >= int(y)


@true_if_left_empty
def new_is_more_fuzziest(x: str, y: str) -> bool:
    return (safer_float(x) - FUZZIEST_TOLERANCE) <= safer_float(y)


def scantype_compare(x: str, y: str) -> bool:
    orig = x.lower()
    new = y.lower()

    return (new == "progressive") or (
        orig in ["mbaff", "interlaced"] and new in ["mbaff", "interlaced"]
    )


X = TypeVar("X")
Y = TypeVar("Y")


def const_true(x: X, y: Y) -> bool:
    return True


def is_sequential(st: set[int]) -> bool:
    """
    Tests if a given set of integers holds is equivalent to [min, max],
    inclusive.
    """
    if len(st) == 0 or len(st) == 1:
        return True
    test_sorted = sorted(st)
    first_item = test_sorted[0]
    for ix, item in enumerate(test_sorted):
        if first_item + ix != item:
            return False
    return True


def sequentialize(st: set[int]) -> set[int]:
    """
    For a given set of integers, returns a set of the same size that is
    sequential and begins at the minimum value.
    """
    if is_sequential(st):
        return st
    size = len(st)
    start = min(st)
    return set(range(start, start + size))


def matches(match_str: str) -> Callable[[str, str], bool]:
    return lambda _, y: y == match_str


def convert_bytes(bytes: float) -> str:
    exp = 1
    while bytes % (1024**exp) < bytes:
        exp += 1
    exp -= 1
    suffixes = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"]
    return "{:.2f} {}iB".format(bytes / (1024**exp), suffixes[exp])


INT_TOLERANCE = 7
FLOAT_TOLERANCE = 0.2
FUZZIEST_TOLERANCE = INT_TOLERANCE + FLOAT_TOLERANCE
CROSS = "✖"
CHECK = "✔"
GAP = "    "

PS = ParamSpec("PS")
R = TypeVar("R")
GEN_CACHE: dict[str, dict[Iterable[Any], Any]] = dict()  # type: ignore[misc]


class ExtraClassProperty:
    def __init__(self, *args: PS.args, **kwargs: PS.kwargs) -> None:
        self.prop_list = args + tuple((key, kwargs[key]) for key in kwargs)


class StreamPropSingleCompareResult:
    """
    Given two media files, this is the difference of a StreamProperty
    over a single Stream, where the Stream is the same type order
    between the files.

    For example, given two files with streams

    .. code-block:: json

    {
      Video: {TypeOrder: 1},
      Audio: {TypeOrder: 1},
      Audio: {TypeOrder: 2},
      Text: {TypeOrder: 1}
    }

    A proper comparison should result in sets of 4
    :py:class:`StreamPropSingleCompareResult`s, one for each
    :py:class:`StreamProperty`. Ultimately, these will be collected into
    :py:class:`StreamPropCompareResult`s, the full collection of which
    stores all the information needed to validate a conversion.
    """

    def __init__(self, type_order: int, f1_json_val: str, f2_json_val: str) -> None:
        self.type_order = type_order
        self.f1_json_val = f1_json_val
        self.f2_json_val = f2_json_val

    def __str__(self) -> str:
        return "StreamPropSingleCompareResult({}, {}, {})".format(
            self.type_order, self.f1_json_val, self.f2_json_val
        )

    def __repr__(self) -> str:
        return self.__str__()


@unique
class StreamProperty(Enum):
    """
    A possible `comparable` key in the json object given by
    :code:`mediainfo --output=JSON ...`. The tuple values are the
    actual name found in the JSON object and the possible JSON
    objects that the property might be found in. For example, for the
    property

    .. code-block:: python

        TypeOrder = _create_val("@typeorder", ["Audio", "Text"])

    this can be found in the output of :code:`mediainfo --output=JSON ...`
    as such:

    .. code-block:: json

        { "@type": "Audio",
          "@typeorder": "..."
          "..."
        },
        { "@type": "Text",
          "@typeorder": "..."
          "..."
        },

    """

    # These need an ordering and a hash to be able to be placed in a set
    @overrides
    def __gt__(self, other: object) -> bool:
        if not isinstance(other, StreamProperty):
            return NotImplemented
        return self.name > other.name

    @overrides
    def __lt__(self, other: object) -> bool:
        if not isinstance(other, StreamProperty):
            return NotImplemented
        return self.name < other.name

    @overrides
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StreamProperty):
            return NotImplemented
        return self.name == other.name

    @overrides
    def __hash__(self) -> int:
        return self.name.__hash__()

    def get_valid_types(self) -> set[StreamType]:
        if not isinstance(self.value[0], set):
            return NotImplemented
        return self.value[0]

    def get_comparison_func(self) -> Callable[[str, str], bool]:
        if not isinstance(self.value[1], types.FunctionType):
            return NotImplemented
        return self.value[1]

    # These are solely for categorization, so the comparison
    # function is const(True)
    Format = (
        set([StreamType.General, StreamType.Video, StreamType.Audio, StreamType.Text]),
        const_true,
        auto(),
    )
    CodecID = (
        set([StreamType.Video, StreamType.Audio, StreamType.Text]),
        const_true,
        auto(),
    )
    StreamOrder = (
        set([StreamType.Video, StreamType.Audio, StreamType.Text]),
        const_true,
        auto(),
    )

    FileSize = (set([StreamType.General]), new_is_less, auto())
    Duration = (
        set([StreamType.General, StreamType.Video, StreamType.Audio]),
        new_is_more_fuzziest,
        auto(),
    )
    FrameRate = (
        set([StreamType.General, StreamType.Video, StreamType.Audio]),
        truthy_if_right_empty(fuzzy_float),
        auto(),
    )
    AudioCount = (set([StreamType.General]), exact_int, auto())
    IsStreamable = (set([StreamType.General]), matches("Yes"), auto())
    Title = (set([StreamType.General]), exact_match, auto())
    VideoCount = (set([StreamType.General]), exact_int, auto())
    BitDepth = (set([StreamType.Video]), exact_match, auto())
    ChromaSubsampling = (set([StreamType.Video]), exact_match, auto())
    ColorSpace = (set([StreamType.Video]), exact_match, auto())
    DisplayAspectRatio = (set([StreamType.Video]), fuzzy_float, auto())
    Height = (set([StreamType.Video]), exact_int, auto())
    PixelAspectRatio = (set([StreamType.Video]), exact_match, auto())
    Sampled_Height = (set([StreamType.Video]), exact_int, auto())
    Sampled_Width = (set([StreamType.Video]), exact_int, auto())
    ScanType = (set([StreamType.Video]), scantype_compare, auto())
    Width = (set([StreamType.Video]), exact_int, auto())
    ChannelLayout = (set([StreamType.Audio]), set_match, auto())
    ChannelPositions = (set([StreamType.Audio]), set_match, auto())
    Channels = (set([StreamType.Audio]), truthy_if_right_empty(exact_int), auto())
    Compression_Mode = (
        set([StreamType.Audio]),
        truthy_if_right_empty(exact_match),
        auto(),
    )
    SamplingRate = (set([StreamType.Audio]), exact_int, auto())
    ServiceKind = (set([StreamType.Audio]), exact_match, auto())

    ## These tests tend to fail:
    FrameCount = (
        set([StreamType.General, StreamType.Video]),
        never_false(almost_int),
        auto(),
    )
    FrameRate_Den = (
        set([StreamType.Video, StreamType.Audio]),
        never_false(exact_int),
        auto(),
    )
    FrameRate_Mode = (set([StreamType.Video]), never_false(exact_match), auto())
    FrameRate_Num = (
        set([StreamType.Video, StreamType.Audio]),
        never_false(exact_int),
        auto(),
    )
    BitRate_Maximum = (set([StreamType.Audio]), never_false(exact_int), auto())


class TermColor(Enum):
    """
    Colors corresponding to terminal escape codes.
    """

    Red = 1
    Green = 2
    Yellow = 3
    Blue = 4
    Magenta = 5
    Cyan = 6
    White = 7


class SwitchDefaults(TypedDict):
    verbosity_limit: int
    color: TermColor
    exact: bool
    inv: bool
    bold: bool
    outstream: TextIO


def cache_choice(
    mayb_cache_dict: Optional[dict[str, dict[Iterable[X], R]]],
) -> Callable[[Callable[PS, R]], Callable[PS, R]]:
    """
    A function wrapper that causes a function's results to be memoized

    Results are stored to a default dictionary, but you can supply your
    own cache dictionary if it's necessary to keep separate caches.

    :param dict cache_dict: The structure that acts as the cache. Any
      data structure with the same interface as a dictionary can be
      used (e.g. a splay tree with the proper functions might improve
      performance)
    """
    cache_dict = mayb_cache_dict if mayb_cache_dict is not None else GEN_CACHE

    def inner(func: Callable[PS, R]) -> Callable[PS, R]:
        def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> R:
            func_name = func.__name__
            arg_tuple: PS.args = args
            kwarg_pairs = tuple((key, kwargs[key]) for key in kwargs)
            try:
                return cache_dict[func_name][arg_tuple + kwarg_pairs]  # type: ignore[no-any-return]
            except KeyError:
                pass

            # printable stuff
            dict_string = tuple(
                (
                    "{}='{}'".format(key, kwargs[key])
                    if isinstance(kwargs[key], str | p.Path)
                    else "{}={}".format(key, kwargs[key])
                )
                for key in kwargs
            )
            arg_string = tuple(
                "'{}'".format(s) if isinstance(s, p.Path | str) else str(s)
                for s in args
            )
            arg_dict_string = ", ".join(arg_string + dict_string)
            print_d(
                "Caching result for {}({})".format(func_name, arg_dict_string),
            )

            # real stuff
            result = func(*args, **kwargs)
            try:
                cache_dict[func_name][arg_tuple + kwarg_pairs] = result
            except KeyError:
                cache_dict[func_name] = dict()
                cache_dict[func_name][arg_tuple + kwarg_pairs] = result
            print_d(type(cache_dict))
            print_d(type(func_name))
            print_d(arg_tuple + kwarg_pairs)

            print_d("=" * shutil.get_terminal_size().columns, init_gap="")

            return result

        wrapper.__doc__ = func.__doc__
        wrapper.__annotations__ = func.__annotations__
        wrapper.__name__ = func.__name__

        return wrapper

    return inner


def cache(func: Callable[PS, R]) -> Callable[PS, R]:
    """
    A function wrapper that causes a function's results to be memoized

    Results are stored to a default dictionary, but you can supply your
    own cache dictionary if it's necessary to keep separate caches.

    :param dict cache_dict: The structure that acts as the cache. Any
      data structure with the same interface as a dictionary can be
      used (e.g. a splay tree with the proper functions might improve
      performance)
    """

    def inner(*args: PS.args, **kwargs: PS.kwargs) -> R:
        func_name = func.__name__
        arg_tuple: PS.args = args
        kwarg_pairs = tuple((key, kwargs[key]) for key in kwargs)
        try:
            return GEN_CACHE[func_name][arg_tuple + kwarg_pairs]  # type: ignore[no-any-return]
        except KeyError:
            pass
        dict_args = tuple("{}={}".format(key, kwargs[key]) for key in kwargs)
        print_d(
            "Caching result for {}{}".format(func_name, args + dict_args),
        )
        result = func(*args, **kwargs)
        try:
            GEN_CACHE[func_name][arg_tuple + kwarg_pairs] = result
        except KeyError:
            GEN_CACHE[func_name] = dict()
            GEN_CACHE[func_name][arg_tuple + kwarg_pairs] = result

        return result

    inner.__doc__ = func.__doc__
    inner.__annotations__ = func.__annotations__
    inner.__name__ = func.__name__

    return inner


def write_timedelta(td: datetime.timedelta) -> str:
    """
    Convert a :class:`datetime.timedelta` object into an easily
    readable string.

    :param datetime.timedelta td: The :class:`datetime.timedelta`
       object to convert.
    :returns: A human readable string equivalent.
    :rtype: str

    e.g.

    .. code-block:: python

      >>> write_timedelta(datetime.timedelta(days=22, hours=30, minutes=102, seconds=195))
      '3 weeks, 2 days, 7 hours, 45 minutes, 15 seconds'

    """

    def reduce_time(multiple: float, unreduced_in_unit: float) -> tuple[float, float]:
        unreduced_mult_unit = unreduced_in_unit / multiple
        remain = multiple * (unreduced_mult_unit - math.floor(unreduced_mult_unit))
        next_unreduced = unreduced_mult_unit - remain / multiple
        return (remain, next_unreduced)

    mults = [
        (60, "seconds"),
        (60, "minutes"),
        (24, "hours"),
        (7, "days"),
        (52.18, "weeks"),
    ]
    remain: float = math.floor(td.total_seconds())
    units = []
    for mult, unit in mults:
        in_unit, remain = reduce_time(mult, remain)
        units.append("{:.2g} {}".format(in_unit, unit))
        if math.fabs(remain) < 1:
            break
    if remain != 0:
        units.append("{:.2g} {}".format(remain, "years"))
    return ", ".join(units[::-1])


def set_fprops(path: p.Path) -> None:
    os.chown(path, CLI_STATE.uid, CLI_STATE.gid)
    os.chmod(path, CLI_STATE.umask)


@cache_choice(GEN_CACHE)
def mediainfo(  # type: ignore[misc]
    path: p.Path, typ: Optional[StreamType] = None
) -> list[dict[StreamProperty, str]]:
    """
    Serialize the output of :code:`mediainfo --output=JSON ...`.

    :param p.Path path: The file path to pass as an argument to :func:`mediainfo`
    :param StreamType typ: The
    :returns:
    :rtype: list[dict[StreamProperty, str]]
    """

    if not isinstance(path, p.Path):
        raise TypeError("mediainfo was passed an object that isn't a pathlib.Path")

    proc_out = s.run(["mediainfo", "--Output=JSON", path], capture_output=True)
    mediainfo_out = proc_out.stdout
    mediainfo_err = proc_out.stderr
    if mediainfo_err:
        raise RuntimeError(mediainfo_err)
    try:
        file_json = json.loads(mediainfo_out)["media"]["track"]
    except TypeError:
        print(f"========================={path}=================")
        print("Full JSON Found")
        print(mediainfo_out.decode("utf-8"))

        # Force the list to keep building
        fake_out: dict[StreamProperty, str] = dict()
        return [fake_out]

    if typ is None:
        return [file_json]

    typ_json_dicts = [obj for obj in file_json if obj["@type"] == typ.name]
    try:
        typ_json_dicts = [obj for obj in typ_json_dicts if float(obj["Duration"]) != 0]
    except KeyError:
        pass
    if typ == StreamType.General:
        if len(typ_json_dicts) != 1:
            raise AssertionError(
                "Number of '{}' objects in '{}' is not one".format(typ, path)
            )

    valid_prop_names = [
        enm.name
        for enm in [enm for enm in list(StreamProperty) if typ in enm.get_valid_types()]
    ]

    return [
        {
            StreamProperty[prop_name]: ""
            if prop_name not in json_dict.keys()
            else json_dict[prop_name]
            for prop_name in valid_prop_names
        }
        for json_dict in typ_json_dicts
    ]


def write_to_width(
    *printable_objects: object,
    width: int = PRINT_WIDTH,
    init_gap: str = GAP,
    subs_gap: str = GAP,
    delim: str = "\n",
) -> str:
    """
    Convert a string to one that wraps to a certain width, with the
    given left padding.

    :param str string: The string to format
    :param int width: The width (number of characters) to wrap at.
    :param str init_gap: The indentation of the first line.
    :param str subs_gap: The indentation of all subsequent lines.
    :param str delim: The delimiter to place between lines. By default,
      this is '\\n'
    :returns: The original string, padded by the given :code:`...gap`
      parameters on the left and wrapped at :code:`width` on the right
    :rtype: str
    """
    string = " ".join(str(x) for x in printable_objects)
    wrapper = TextWrapper(
        initial_indent=init_gap,
        break_long_words=False,
        width=width,
        subsequent_indent=subs_gap,
    )
    return delim.join(
        [delim.join(wrapper.wrap(line)) for line in string.splitlines(True)]
    )


def print_to_width(
    *printable_objects: object,
    init_gap: str = GAP,
    subs_gap: str = GAP,
    delim: str = "\n",
    outstream: TextIO = sys.stdout,
) -> None:
    """
    Print a string, wrapping to a certain width, with the
    given left padding.

    :param str string: The string to format
    :param int width: The width (number of characters) to wrap at.
    :param str init_gap: The indentation of the first line.
    :param str subs_gap: The indentation of all subsequent lines.
    :param str delim: The delimiter to place between lines. By default,
      this is :code:`"\\\\n"`
    :param TextIO outstream: The file stream to write to
      (stdout by default).
    :returns: None
    """
    string = " ".join(str(x) for x in printable_objects)
    outstream.write(
        write_to_width(
            string,
            init_gap=init_gap,
            subs_gap=subs_gap,
            delim=delim,
        )
    )
    outstream.write("\n")
    outstream.flush()


def print_d(
    *args: PS.args,
    **kwargs: PS.kwargs,
) -> None:
    """
    Print a string in DEBUG mode, possibly above a certain verbosity.
    Any arg that contains already formatted text (through color_text)
    will be preserved.

    :param list[Any] args: A list of arguments to be printed, where they
      will be separated by spaces.
    :param int verbosity_limit: Sets the verbosity at (or above) which
      the string will be printed.
    :param color: Color the text using the given terminal color.
    :type color: :class:`TermColor`
    :param bool inv: If true, flip the background and foreground colors.
    :param bool bold: If true, bold the text.
    :param outstream: The file stream to write to
      (stdout by default).
    :type outstream: :class:`TextIO`
    :returns: None
    """
    defaults: SwitchDefaults = {
        "verbosity_limit": 1,
        "color": TermColor.Blue,
        "exact": False,
        "inv": False,
        "bold": False,
        "outstream": sys.stdout,
    }

    @overload
    def get_kwarg(key: Literal["exact"]) -> bool:
        return get_kwarg(key)

    @overload
    def get_kwarg(key: Literal["verbosity_limit"]) -> int:
        return get_kwarg(key)

    @overload
    def get_kwarg(key: Literal["color"]) -> TermColor:
        return get_kwarg(key)

    @overload
    def get_kwarg(key: Literal["inv", "bold"]) -> bool:
        return get_kwarg(key)

    @overload
    def get_kwarg(key: Literal["outstream"]) -> TextIO:
        return get_kwarg(key)

    def get_kwarg(
        key: Literal["verbosity_limit", "color", "inv", "bold", "outstream", "exact"],
    ) -> object:
        return defaults[key] if key not in kwargs.keys() else kwargs[key]

    verbosity_limit = get_kwarg("verbosity_limit")
    color = get_kwarg("color")
    inv = get_kwarg("inv")
    bold = get_kwarg("bold")
    outstream = get_kwarg("outstream")

    if verbosity_limit > CLI_STATE.verbosity:
        return
    d_str = GAP + "  " + D_STRING
    blank = " " * (len(d_str) - 2) + D_STRING[2:]
    result = write_to_width(
        " ".join(map(str, args)), init_gap=d_str, subs_gap=blank, delim="\n"
    )
    color_val = 8
    if color is not None:
        color_val = color.value
    color_prefix = 4 if inv else 3
    bold_code = 1 if bold else 0
    escape_code = "[{};{}{}m".format(bold_code, color_prefix, color_val)
    return_code = "[0m"
    new_result = []
    next_escape = escape_code
    inner_color = False
    for line in result.splitlines():
        line = re.sub("\[0m(?!|$)", f"[0m{escape_code}", line)
        line = re.sub(d_str, f"{d_str}{next_escape}", line)
        line = re.sub(blank, f"{blank}{next_escape}", line)
        escapes = re.findall("\[[0-9];[0-9]{2}m", line)
        last_escape_in_line = ""
        inner_color = False
        if escapes:
            last_escape_in_line = escapes[-1]
            inner_color = False
            try:
                inner_color = line.index(last_escape_in_line) > line.index("[0m")
            except ValueError:
                inner_color = False
        next_escape = last_escape_in_line if inner_color else escape_code
        new_result.append(line + return_code)

    outstream.write("\n".join(new_result))
    outstream.write("\n\n")
    outstream.flush()


def shorten_string(string: str, length: int) -> str:
    """
    Cut off a string at the given length, replacing the end with an
    ellipsis.

    :param str string: The string to cut.
    :param int length: The length of the new string (including ellipsis).
    :returns: The original string, with an ellipsis, cut to the given length.
    """
    string = str(string)
    if len(string) <= length:
        return string
    if length <= 3:
        return "..."
    return string[: length - 3] + "..."


def string_val_to_set(string: str) -> set[str]:
    """
    Convert the value of a :func:`mediainfo` json property to a set.

    Consider the property :code:`ChannelPositions`. This is given
    as a string, e.g. :code:`"Front: L C R, Back: L R, LFE"`. In
    this case the order of :code:`"Front"` and :code:`"Back"` don't
    matter, it's just the fact that both files have the same values
    for each. This function splits such a string into
    :code:`{"Front: L C R", "Back: L R", "LFE"}`.

    :param str string: The value of a stream property in the
      output of :func:`mediainfo`.
    :returns: The above value split into a set of "important" tokens.
    :rtype: set[str]
    """
    by_comma = string.split(",")
    if len(by_comma) > 1:
        key_pairs = [key_pair.split(":") for key_pair in by_comma]
        return set(chain.from_iterable([set(key_val) for key_val in key_pairs]))
    return set(string.split(" "))


def color_text(
    string: str, color: TermColor, inv: bool = False, bold: bool = False
) -> str:
    """
    Changes a string's color.

    :param str string: The input string
    :param color: The (ascii terminal) color
    :type color: :class:`TermColor`
    :param bool inv: If true, change the background color instead.
    :param bool bold: If true, bold the text and use the bolded color code.
    :returns: The string, wrapped with ANSI terminal codes for the given color text.
    :rtype: str
    """
    if CLI_STATE.no_color:
        return string
    color_val = color.value
    color_prefix = 4 if inv else 3
    bold_code = 1 if bold else 0
    escape_code = "[{};{}{}m".format(bold_code, color_prefix, color_val)
    return_code = "[0m"
    # Replace all end escape codes (^[[0m) in the original string
    # with a new color following them (^[[0m^[[...), unless they
    # end the string or are already followed by a color
    colors_preserved = re.sub("\\[0m(?!|$)", "[0m" + escape_code, string)
    return escape_code + colors_preserved + return_code


@cache_choice(GEN_CACHE)
def get_pairs(  # type: ignore[misc]
    fname1: p.Path, fname2: p.Path, typ: StreamType
) -> list[tuple[dict[StreamProperty, str], dict[StreamProperty, str]]]:
    """
    Return a tuple of `mediainfo` dictionaries of the given type.

    :param p.Path fname1: The original file to compare.
    :param p.Path fname2: The converted file.
    :param p.Path type: The type of media stream to compare.
    :returns: A list of pairs, where each member of the pair is the
      JSON output of :func:`mediainfo` for the given type.
    :rtype: list[tuple[dict[StreamProperty, str], dict[StreamProperty, str]]]
    """
    f1 = mediainfo(fname1, typ=typ)
    f2 = mediainfo(fname2, typ=typ)
    results = []
    for ix in range(len(f1)):
        try:
            f1_dict = f1[ix]
        except IndexError:
            f1_dict = dict()
        try:
            f2_dict = f2[ix]
        except IndexError:
            f2_dict = dict()
        results.append((f1_dict, f2_dict))
    return results


def compare_stream_property(
    pairs: list[tuple[dict[StreamProperty, str], dict[StreamProperty, str]]],
    key: StreamProperty,
    compare_func: Callable[[str, str], bool | Tern],
) -> tuple[Tern | bool, list[StreamPropSingleCompareResult]]:
    """
    Compare the values of :func:`mediainfo`'s output.

    :param pairs: A tuple of the output of
      :func:`mediainfo`, where the first value is from the original file
      and the second value is the converted file.
    :type pairs: list[tuple[dict[StreamProperty, str], dict[StreamProperty, str]]]
    :param key: The json key to compare.
    :type key: StreamProperty
    :param Callable[[str, str], bool] compare_func: The function
      used to compare the values in :code:`pairs`
    :returns: True if the values of the pairs for the given key are equal,
      according to the :code:`compare_func`
    """
    olds = []
    news = []
    try:
        # TODO: This will not keep the order between `news` and
        # `olds`. Use a default if x[0][key] is not found (or x[1][key])
        # olds = [x[0][key] for x in pairs if key in x[0]]
        olds = [x[0][key] if key in x[0] else "" for x in pairs]
    except KeyError:
        olds = []
    try:
        # news = [x[1][key] for x in pairs if key in x[1]]
        news = [x[1][key] if key in x[1] else "" for x in pairs]
    except KeyError:
        news = []
    try:

        def possible_val(ix: int, lst: list[str]) -> str:
            return "" if len(lst) <= ix else lst[ix]

        results = [
            (
                compare_func(possible_val(ix, olds), possible_val(ix, news)),
                StreamPropSingleCompareResult(
                    ix + 1,
                    possible_val(ix, olds),
                    possible_val(ix, news),
                ),
            )
            for ix in range(max(len(olds), len(news)))
        ]
    except AttributeError:
        return (False, [])
    except TypeError:
        return (False, [])
    bad_results = [tup[1] for tup in results if Tern(tup[0]) != Tern.TTrue]
    iffy_results = [tup[1] for tup in results if tup[0] == Tern.TOther]
    return (
        Tern.TOther if len(iffy_results) != 0 else len(bad_results) == 0,
        bad_results,
    )


@cache_choice(GEN_CACHE)
def verify_conversion_tests(  # type: ignore[misc]
    fname1: p.Path, fname2: p.Path
) -> list[
    tuple[bool | Tern, StreamType, StreamProperty, list[StreamPropSingleCompareResult]]
]:
    """
    Verify the conversion of a media file.

    :param p.Path fname1: The original media file
    :param p.Path fname2: The converted media file
    :returns: A list of failed tests and their results as a tuple where the entries are as follows:
        (result (true if any of the comparisons are not bool(x) == True), type of stream, property to compare, stream indices, mismatched output)
    :rtype: list[tuple[bool, StreamType, StreamProperty, list[StreamPropSingleCompareResult]]]
    """
    if not (fname1.exists() and fname2.exists()):
        raise IOError("Comparison can't proceed because file doesn't exist.")

    tests = list(StreamProperty)
    if any(
        json[StreamProperty.Format] == "Opus"
        for json in mediainfo(fname2, typ=StreamType.Audio)
    ):
        tests = [
            prop
            for prop in tests
            if not (
                StreamType.Audio in prop.get_valid_types()
                and prop.name
                in ["ChannelLayout", "ChannelPositions", "Channels", "FrameRate"]
            )
        ]

    results = [
        [
            (
                compare_stream_property(
                    get_pairs(fname1, fname2, typ), prop, prop.get_comparison_func()
                ),
                typ,
                prop,
            )
            for typ in prop.get_valid_types()
        ]
        for prop in tests
    ]
    orig_results = []
    for comparison_lst in results:
        for value_and_compare_results, typ, prop in comparison_lst:
            verified, cmp_result = value_and_compare_results
            orig_results.append((verified, typ, prop, cmp_result))
    return orig_results


def verify_conversion(fname1: p.Path, fname2: p.Path) -> bool | Tern:
    """
    Verify that the two files match, up to the tests in :code:`STREAM_PROP_TESTS`.

    :returns: True if :code:`fname1` ~= :code:`fname2`
    """
    results = [tup[0] for tup in verify_conversion_tests(fname1, fname2)]
    if False in results:
        return False
    if Tern.TOther in results:
        return Tern.TOther
    return True


def failed_tests(fname1: p.Path, fname2: p.Path) -> str:
    """
    Return a 'prettified' string detailing the tests that :code:`fname1`
      and :code:`fname2` do not match on.
    """
    results = verify_conversion_tests(fname1, fname2)

    if [
        json_prop
        for json_props in [
            mediainfo(fname2, x)
            for x in [StreamType.Video, StreamType.Audio, StreamType.Text]
        ]
        for json_prop in json_props
    ] == []:
        return color_text("No valid stream properties.", TermColor.Red)

    pretty_results = "\n".join(
        tup[1].name
        + "."
        + tup[2].name
        + str([comparison.type_order for comparison in tup[3]])
        + ": "
        + "["
        + ", ".join(
            color_text(
                str((comparison.f1_json_val, comparison.f2_json_val)),
                (
                    TermColor.Red
                    if not tup[2].get_comparison_func()(
                        comparison.f1_json_val, comparison.f2_json_val
                    )
                    else TermColor.Blue
                ),
            )
            for comparison in tup[3]
        )
        + "]"
        for tup in results
        if tup[0] == Tern.TOther or not tup[0]
    )

    return pretty_results


def get_converted_name(path: p.Path) -> p.Path:
    """
    Give the name used for the new file in the conversion of :code:`path`
    """
    output_ext = CLI_STATE.container.output_ext()
    if path.suffix not in CLI_STATE.container.valid_exts():
        return path.with_suffix(output_ext)
    else:
        return path.with_suffix("." + COLLISION_SUFFIX + path.suffix)


def is_skip_codec(path: p.Path) -> bool:
    """
    Whether the given file should not be converted, on the basis of its :code:`Format`.
    """
    vid_jsons = mediainfo(path, typ=StreamType.Video)
    codecs = [obj[StreamProperty.Format] for obj in vid_jsons]
    return any(codec in SKIP_CODECS for codec in codecs)


def is_video_container(path: p.Path) -> bool:
    gen_json = mediainfo(path, typ=StreamType.General)[0]
    container = None
    try:
        container = gen_json[StreamProperty.Format]
    except KeyError:
        return False
    if container == "":
        return False
    return True


def is_already_converted(path: p.Path) -> bool:
    """
    Whether the given file is already the chosen codec in the chosen container.
    """
    gen_json = mediainfo(path, typ=StreamType.General)[0]
    container = None
    try:
        container = gen_json[StreamProperty.Format]
    except KeyError:
        raise (ValueError("File {} doesn't look like a video file".format(path)))
    if container != CLI_STATE.container.json_name():
        return False
    vid_jsons = mediainfo(path, typ=StreamType.Video)
    codecs = [obj[StreamProperty.Format] for obj in vid_jsons]
    codec_ids = [obj[StreamProperty.CodecID] for obj in vid_jsons]
    return all(codec == "HEVC" for codec in codecs) and all(
        codec_id == "hvc1" or codec_id == "hev1" for codec_id in codec_ids
    )


def validate_streams_to_convert(
    path: p.Path, typ: StreamType, num_packets: int = 200
) -> tuple[set[int], set[int]]:
    """
    Using :code:`ffprobe`, finds streams that will cause the x265 codec
    to fail on the basis of an invalid :code:`duration` parameter.

    :param p.Path path: The file to query.
    :param StreamType typ: The :class: StreamType to validate.
    :param int num_packets: The number of packets to scan (from the
      beginning of the file) for streams.
    :returns: A tuple, the first value of which contains a list of
      valid stream indices, and the second a list of invalid indices.
    """
    ## This will not grab tracks that aren't in the first `num_packets`
    ## packets
    # TODO: Clean up the two attempts
    ffprobe_cmd = [
        "ffprobe",
        "-loglevel",
        "error",
        "-read_intervals",
        "0:00%+#" + str(num_packets),
        "-select_streams",
        typ.ffprobe_ident(),
        "-show_entries",
        "packet=stream_index,duration",
        "-of",
        "csv",
        str(path.absolute()),
    ]
    ffprobe_out = s.run(
        ffprobe_cmd,
        capture_output=True,
    )
    ffprobe_stderr = ffprobe_out.stderr.decode("utf-8")
    if ffprobe_stderr:
        ffprobe_cmd = ffprobe_cmd[:3] + ffprobe_cmd[5:]
        print_to_width("ffprobe failure:")
        print_to_width(ffprobe_stderr, init_gap=2 * GAP, subs_gap=2 * GAP)
        print_to_width("Attempting again without seeking through stream")
        ffprobe_out = s.run(
            ffprobe_cmd,
            capture_output=True,
        )
    ffprobe_stderr = ffprobe_out.stderr.decode("utf-8")
    if ffprobe_stderr:
        err = write_to_width(
            "ffprobe (2nd) failure:" + "\n" + ffprobe_stderr,
            init_gap=2 * GAP,
            subs_gap=2 * GAP,
        )
        raise RuntimeError(err)

    ffprobe_stdout = [
        packet_lst
        for packet_lst in [
            packet_str.split(",")
            for packet_str in ffprobe_out.stdout.decode("utf-8").split("\n")
        ]
        if len(packet_lst) == 3
    ]
    all_stream_ixs = set(int(packet_lst[1]) for packet_lst in ffprobe_stdout)
    invalid_ixs = set(
        int(packet_lst[1]) for packet_lst in ffprobe_stdout if packet_lst[2] == "N/A"
    )
    return (all_stream_ixs, invalid_ixs)


def generate_conversions(
    path: p.Path, typ: StreamType, validate: bool = False
) -> list[str]:
    """Given a file, generate the ffmpeg string to convert all subtitles
     appropriately.

    :param path: The input file path
    :type path: :class:`pathlib.Path`

    :returns: A list of arguments to be passed to ffmpeg
    :rtype: :py:class:`list[str]`

    ffmpeg cannot convert subtitles between images and text, so the
    only conversions allowed are text->text and bitmap->bitmap.
    The examples below explain what format ffmpeg expects and how this
    function generates its result.

    How ffmpeg must convert subtitles
    ---------------------------------
    Get all objects with :code:`@type: text`, pull their :code:`@typeorder`
    and :code:`CodecID`. Subtract 1 from :code:`@typeorder`. Then, if
    :code:`"CodecID": "S_TEXT/UTF8"` use :code:`mov_text`, otherwise
    :code:`dvdsub`:

      Example 1:

      .. code-block:: json

        {
          "@type": "Text",
          "@typeorder": "2",
          "CodecID": "S_HDMV/PGS",
        }

      .. code-block:: bash

        $> ffmpeg ... -map 0:s:1 -c:s:1 dvdsub ...

      Example 2:

      .. code-block:: json

        {
          "@type": "Text",
          "@typeorder": "5",
          "CodecID": "S_TEXT/UTF8"
        }

      .. code-block:: bash

        $> ffmpeg ... -map 0:s:4 -c:s:4 mov_text ...

    """

    def clean_stream_order(stream_order: str) -> str:
        return stream_order if stream_order[:2] != "0-" else stream_order[2:]

    num_frames = 200
    codec_ids = [
        typ_json[StreamProperty.CodecID] for typ_json in mediainfo(path, typ=typ)
    ]

    if validate:
        try:
            all_stream_ixs, invalid_ixs = validate_streams_to_convert(
                path, typ, num_packets=num_frames
            )
        except RuntimeError as e:
            print(e)
            validate = False
    if not validate:
        invalid_ixs = set()
        all_stream_ixs = set(
            int(clean_stream_order(sub_json[StreamProperty.StreamOrder]))
            for sub_json in mediainfo(path, typ=typ)
        )

    # If 200 packets is too small, keep tryin'
    more_frames = False
    while validate and len(all_stream_ixs) != len(mediainfo(path, typ=typ)):
        num_frames *= 2
        if not more_frames:
            print_to_width(
                "Calculated stream indexes ({}) for type '{}' do not match the mediainfo output({})".format(
                    len(all_stream_ixs), typ, len(mediainfo(path, typ=typ))
                ),
                init_gap=2 * GAP,
                subs_gap=2 * GAP,
            )
        sys.stdout.write(
            write_to_width(
                "Searching {} for streams -> ".format(num_frames),
                init_gap=2 * GAP,
                subs_gap=2 * GAP,
            )
        )
        all_stream_ixs, invalid_ixs = validate_streams_to_convert(
            path, typ, num_packets=num_frames
        )
        print(len(all_stream_ixs))
        more_frames = True

    # Only matters if validate is True (otherwise this is obviously
    # True)
    all_stream_ixs_json = set(
        int(clean_stream_order(sub_json[StreamProperty.StreamOrder]))
        for sub_json in mediainfo(path, typ=typ)
    )
    print_d("typ, validate:", typ, validate, verbosity_limit=3)
    print_d(
        "all_ixs, all_ixs_json:", all_stream_ixs, all_stream_ixs_json, verbosity_limit=3
    )
    if all_stream_ixs != all_stream_ixs_json:
        raise (
            AssertionError(
                f"The calculated '{typ}' indices found do not match those given by mediainfo"
            )
        )

    valid_stream_ixs = all_stream_ixs.difference(invalid_ixs)
    valid_stream_ixs = sequentialize(valid_stream_ixs)
    invalid_ixs = sequentialize(invalid_ixs)
    encoding_args = []
    try:
        offset = min(all_stream_ixs)
    except ValueError:
        offset = 0
    for num_codecs_so_far, stream_ix in enumerate(valid_stream_ixs):
        type_ix = stream_ix - offset
        # TODO: Fix for when audio stream ix's > video's
        codec_id = codec_ids[type_ix]
        encoding = CODEC_ID_MAP[codec_id]

        encode_key = typ.ffprobe_ident()
        encoding_args += [
            "-map",
            f"0:{encode_key}:{type_ix}",
            f"-c:{encode_key}:{num_codecs_so_far}",
            f"{encoding}",
        ]
    return encoding_args


def pprint_ffmpeg(path: p.Path, width: int = 0) -> str:
    """
    Pretty print the ffmpeg command that will be used to convert the
    given file.
    """
    if width == 0:
        width = shutil.get_terminal_size().columns
    lst = ffmpeg_cmd(path).copy()
    ix = lst.index(str(path))
    lst[ix] = '"' + lst[ix] + '"'
    lst[-1] = '"' + lst[-1] + '"'
    delim = " \\ \n"

    return delim.join(
        (
            write_to_width(" ".join(lst[0:ix]), delim=delim, width=width),
            2 * GAP + f"{lst[ix]}",
            write_to_width(
                " ".join(lst[ix + 1 : -1]),
                init_gap=2 * GAP,
                subs_gap=2 * GAP,
                delim=delim,
                width=width - 5,
            ),
            2 * GAP + f"{lst[-1]}",
        )
    )


@cache_choice(GEN_CACHE)
def ffmpeg_cmd(path: p.Path, skip_subs: bool = False) -> list[str]:  # type: ignore[misc]
    video_tracks = mediainfo(path, typ=StreamType.Video)
    heights = set([track[StreamProperty.Height] for track in video_tracks])
    widths = set([track[StreamProperty.Width] for track in video_tracks])

    # TODO: These may not be the same index
    height = max(heights)
    width = max(widths)

    # Without a duration field, validation will fail
    subtitles_have_duration = True
    subtitle_conversions = []
    try:
        [
            track[StreamProperty.Duration]
            for track in mediainfo(path, typ=StreamType.Text)
        ]
    except KeyError:
        subtitles_have_duration = False
    if subtitles_have_duration:
        subtitle_conversions = generate_conversions(
            path, StreamType.Text, validate=True
        )
    else:
        subtitle_conversions = generate_conversions(path, StreamType.Text)

    cmd = [
        "ffmpeg",
        "-fix_sub_duration",
        "-canvas_size",
        f"{width}x{height}",
        "-i",
        f"{path}",
        "-movflags",
        "+faststart",
        "-map_metadata",
        "0",
        "-strict",
        "-2",
    ]
    cmd += generate_conversions(path, StreamType.Video)
    cmd += generate_conversions(path, StreamType.Audio)
    if not skip_subs:
        cmd += subtitle_conversions
    cmd += [str(get_converted_name(path))]
    return cmd


def signal_notify(
    message: str, sender: str = phone_number, recipients: list[str] = [phone_number]
) -> None:
    post_data = {
        "message": message,
        "number": sender,
        "recipients": recipients,
    }
    post_headers = {"Content-Type": "application/json"}
    requests.post("http://localhost:9922/v2/send", json=post_data, headers=post_headers)


def process_vidlist(
    vidlist: list[p.Path],
    exact: bool = False,
    mayb_limit: Optional[int] = None,
    convert_and_replace: bool = True,
    keep_failures: bool = False,
    just_list: bool = False,
    dry_run: bool = False,
) -> float:
    global TOTAL_SAVED
    global CURRENT_CONVERT
    global CURRENT_OBJECT
    total_size_saved = 0
    limit = len(vidlist) if mayb_limit is None else min(len(vidlist), mayb_limit)
    exact_limit = 0
    if limit < 10:
        exact = True

    if exact:
        for file in vidlist:
            if is_already_converted(file):
                continue
            if exact_limit > limit:
                break
            exact_limit += 1
        limit = min(limit, exact_limit)

    limit_digits = len(f"{limit}")

    num_processed = 0
    for cur_path in vidlist:
        CURRENT_CONVERT = p.Path("")
        CURRENT_OBJECT = p.Path("")
        # Because the size of the vidlist is iffy
        if num_processed >= limit:
            return total_size_saved

        if not is_video_container(cur_path):
            continue
        if is_already_converted(cur_path):
            continue
        if just_list:
            print(os.path.getsize(cur_path), cur_path)
            continue

        new_path = get_converted_name(cur_path)
        format_string = "({:" + f"{limit_digits}d" + "}/{:d}) Processing {}"

        progress_string = format_string.format(num_processed + 1, limit, cur_path.name)

        sep_char = "=" if first_sep else "-"
        first_sep = False
        print_to_width(sep_char * shutil.get_terminal_size().columns, init_gap="")
        shutil.get_terminal_size().columns

        print_to_width(
            progress_string,
            init_gap="",
            subs_gap=GAP,
        )
        old_size = os.path.getsize(cur_path)
        size_prefix = "Size: "
        print_to_width(
            "{}{}".format(size_prefix, convert_bytes(old_size)),
            subs_gap=GAP + "".join(" " for _ in size_prefix),
        )
        dir_prefix = "Dir: "
        print_to_width(
            "{}{}".format(dir_prefix, cur_path.parent),
            subs_gap=GAP + "".join(" " for _ in dir_prefix),
        )
        new_file_prefix = "New file: "
        print_to_width(
            "{}{}".format(new_file_prefix, new_path),
            subs_gap=GAP + "".join(" " for _ in new_file_prefix),
            delim="\\ \n",
        )
        print_to_width()
        if is_skip_codec(cur_path):
            print_to_width(color_text("AV1/VP9 file!", TermColor.Green))
            print_to_width()
            continue

        if os.path.exists(new_path):
            print_to_width("Found existing conversion, verify then skip:")
            verified = verify_conversion(cur_path, new_path)
            verif_color: Callable[[str], str] = (
                (lambda x: color_text(x, TermColor.Green))
                if verified
                else (lambda x: color_text(x, TermColor.Red))
            )
            verif_mark = CHECK if verified else CROSS
            verified_str = write_to_width(f"{verif_color(verif_mark)} Verified")
            print(verified_str)
            if (not verified and isinstance(verified, bool)) or verified == Tern.TOther:
                print_to_width(
                    failed_tests(cur_path, new_path),
                    init_gap=GAP + "  ",
                    subs_gap=2 * GAP,
                    delim=",\n",
                )

            if (verified and isinstance(verified, bool)) or verified == Tern.TOther:
                VERIFIED_TRANSCODES.append(cur_path)

            print_to_width()
            continue

        signal_notify("{} ({})".format(progress_string, convert_bytes(old_size)))

        ffmpeg_proc_str = ffmpeg_cmd(cur_path)

        print(pprint_ffmpeg(cur_path))
        print_to_width()

        if dry_run:
            num_processed += 1
            continue

        CURRENT_OBJECT = cur_path
        CURRENT_CONVERT = new_path
        before = datetime.datetime.now()
        ffmpeg_return_code = ffpb.main(argv=ffmpeg_proc_str[1:], stream=sys.stderr)
        if ffmpeg_return_code != 0:
            print_to_width(f"{color_text(CROSS, TermColor.Red)} ffmpeg Failed")
            print_to_width("---")
            print_to_width("Retrying without subtitles")
            os.remove(new_path)
            ffmpeg_proc_str = ffmpeg_cmd(cur_path, skip_subs=True)
            ffmpeg_return_code = ffpb.main(argv=ffmpeg_proc_str[1:], stream=sys.stderr)
        after = datetime.datetime.now()
        time_taken = after - before
        print_to_width(f"Conversion Runtime: {write_timedelta(time_taken)}")
        set_fprops(new_path)

        verified = verify_conversion(cur_path, new_path)
        if (verified and isinstance(verified, bool)) or verified == Tern.TOther:
            old_size = os.path.getsize(cur_path)
            new_size = os.path.getsize(new_path)
            diff_size = old_size - new_size
            total_size_saved += diff_size
            TOTAL_SAVED += diff_size

            compression = find_compression_ratio(cur_path, new_path)

            signal_notify(
                "✅ Verified {}\n{new_size} • {perc:.2f}%".format(
                    new_path.name, new_size=convert_bytes(new_size), perc=compression
                )
            )
            print_to_width(f"{color_text(CHECK, TermColor.Green)} Metadata matches")
            print_to_width(
                f"Compression: {compression:.3f}%", init_gap=(2 * GAP + "  ")
            )
            print_to_width(
                f"Savings: {convert_bytes(diff_size)} = {convert_bytes(old_size)} - {convert_bytes(new_size)}",
                init_gap=2 * GAP + "  ",
            )
            if diff_size < 0:
                print_to_width(color_text("No savings!", TermColor.Red))
                print_to_width(f"Keeping {color_text(str(new_path), TermColor.Red)}")
                num_processed += 1
                continue
            if cur_path.suffix in QUESTIONABLE_EXTS:
                print_to_width(
                    color_text(f"Old extension: '{cur_path.suffix}", TermColor.Magenta)
                )
                print_to_width(
                    f"Keeping {color_text(str(cur_path), TermColor.Magenta)}"
                )
                num_processed += 1
                continue

            if (
                convert_and_replace
                and cur_path.suffix in CLI_STATE.container.valid_exts()
            ):
                os.replace(new_path, cur_path)
            elif convert_and_replace:
                os.remove(cur_path)
            elif cur_path.suffix in CLI_STATE.container.valid_exts():
                print_to_width(
                    f"os.replace({new_path}, {cur_path})",
                    subs_gap=2 * GAP,
                    delim=" \\\n",
                )
            else:
                print_to_width(
                    f"os.remove({cur_path})", subs_gap=2 * GAP, delim=" \\\n"
                )
        else:
            failed = failed_tests(cur_path, new_path)
            failed_lst = failed.splitlines()
            num_failed = len(failed_lst)
            webhook_message = (
                " • ".join(failed_lst)
                if num_failed < 4
                else " • ".join(failed_lst[:3]) + "..."
            )
            webhook_message = uncolor_text(webhook_message)
            signal_notify("❌ Failed {}\n{}".format(new_path.name, webhook_message))

            print_to_width(f"{color_text(CROSS, TermColor.Red)} Metadata mismatch")
            print_to_width(
                failed,
                init_gap=GAP + "  ",
                subs_gap=2 * GAP,
                delim=",\n",
            )
            if not keep_failures:
                os.remove(new_path)
                print_to_width()
            else:
                print_to_width(f"Keeping {color_text(str(new_path), TermColor.Red)}")
                print_to_width()
                num_processed += 1
            continue

        num_processed += 1
        print_to_width()
    # Because the size of the vidlist is accurate
    return total_size_saved


def human_readable(num: float, suffix: str = "B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return "{:3.1f}{}{}".format(num, unit, suffix)
        num /= 1024.0
    return "{:.1f}Yi{}".format(num, suffix)


def find_compression_ratio(f1: p.Path, f2: p.Path) -> float:
    f1_size = os.path.getsize(f1)
    f2_size = os.path.getsize(f2)
    return 100 * f2_size / f1_size


def main() -> None:
    global TIME_START
    # Retrieve the program state from arguments
    exact = CLI_STATE.exact
    just_list = CLI_STATE.list_files
    keep_failures = CLI_STATE.keep_failures
    replace = not CLI_STATE.keep_original
    dry_run = CLI_STATE.dry_run
    num_files = CLI_STATE.num_files
    for key, val in CODEC_ID_MAP.items():
        if val is None:
            CODEC_ID_MAP[key] = CLI_STATE.encoder

    # Process given files
    files = [file for file in CLI_STATE.FILES if file.is_file()]
    # Check if the files are media files
    nonmedia_files = set(
        file
        for file in files
        if len(VIDEO_FILE_EXTS.intersection(set(file.suffixes))) == 0
    )
    if nonmedia_files:
        raise (
            ValueError(
                f"The files {nonmedia_files} can not be converted (must have ext in {VIDEO_FILE_EXTS})."
            )
        )

    # Process given folders
    folders = [file for file in CLI_STATE.FILES if file.is_dir()]

    def glob_folder(path: p.Path) -> list[p.Path]:
        return list(
            chain.from_iterable(
                [list(path.glob(f"**/*{ext}")) for ext in VIDEO_FILE_EXTS]
            )
        )

    globbed = list(map(glob_folder, folders))
    accumulated_globs = []
    if globbed:
        accumulated_globs = list(chain.from_iterable(globbed))
    vid_list = list(files) + accumulated_globs
    assert len(files) + len(folders) == len(CLI_STATE.FILES)

    # Start the clock
    orig_time = datetime.datetime.now()
    TIME_START = orig_time

    files_and_sizes: list[tuple[p.Path, int]] = [
        (file, os.path.getsize(file)) for file in vid_list
    ]
    files_and_sizes = sorted(files_and_sizes, key=lambda video_size: -video_size[1])

    videos = [video_size_tup[0] for video_size_tup in files_and_sizes]
    total_size_saved = process_vidlist(
        videos,
        exact=exact,
        mayb_limit=num_files,
        convert_and_replace=replace,
        just_list=just_list,
        keep_failures=keep_failures,
        dry_run=dry_run,
    )
    print_to_width("=" * shutil.get_terminal_size().columns, init_gap="")
    total_time_taken = datetime.datetime.now() - TIME_START

    if just_list:
        print_d(GEN_CACHE, verbosity_limit=4)
        return

    print(
        "Total Savings:",
        str(convert_bytes(total_size_saved)),
        "(" + str(total_size_saved) + ")",
    )
    print(
        "Total Time:",
        write_timedelta(total_time_taken),
        "(" + str(total_time_taken) + ")",
    )


def setup_argparse() -> ap.ArgumentParser:
    def replace_space(string: str) -> str:
        while "  " in string:
            string = string.replace("  ", "")
        while ";;" in string:
            string = string.replace(";;", " ")
        return string

    parser = ap.ArgumentParser(
        formatter_class=ap.RawTextHelpFormatter,
        prog="mass_convert",
        description=replace_space(
            """Batch convert video files to H.265/HEVC (MP4 container).
        By default, this will search for all video files under the current path,
        converting them in order of filesize (largest to smallest)

        Note: While the default encoder tends to give the best filesizes,
        it may fail more often than the others. In order of preference:
        ;;;;;;;;libx265 > libsvt_hevc > hevc_nvenc
        """
        ),
    )
    parser.add_argument(
        "FILES",
        default=[],
        nargs="*",
        type=p.Path,
        action="extend",
        help="A list of files to encode. (Overrides default behavior)",
    )
    parser.add_argument(
        "-n",
        "--num-files",
        action="store",
        type=int,
        help="Only convert the `n' largest files",
    )
    parser.add_argument(
        "-c",
        "--container",
        choices=[VidContainer.Matroska, VidContainer.MPEG_4],
        type=VidContainer,
        default=VidContainer.MPEG_4,
        help="The container to use for the output file",
    )
    parser.add_argument(
        "-e",
        "--encoder",
        choices=[
            "libx265",
            "hevc_nvenc",
            "libsvt_hevc",
            "hevc_v4l2m2m",
            "hevc_vaapi",
            "hevc_qsv",
        ],
        default="libx265",
        help="The video encoder to use.",
    )
    parser.add_argument(
        "-x",
        "--exact",
        action="store_true",
        help="""Calculate the exact number of files that will be converted
This is automatically set to true if the number of files is smaller than 10""",
    )
    parser.add_argument(
        "-o",
        "--keep-original",
        action="store_true",
        help="If set, don't remove the original file after encoding.",
    )
    parser.add_argument(
        "-f",
        "--keep-failures",
        action="store_true",
        help="If set, don't remove the mp4 files from a failed ffmpeg conversion",
    )
    parser.add_argument(
        "-l",
        "--list-files",
        action="store_true",
        help="Only print out a list of (bytes, filename) pairs",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Do everything in a normal run, except convert and verify (Useful for finding ffmpeg args).",
    )
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Print out debug statements. This will be more verbose if more arguments are used.",
    )
    parser.add_argument(
        "--umask",
        action="store",
        default=UMask("0113").mask,
        type=str,
        help="Set the umask for any files created. Default is 0113 (u=rw, g=rw, o=r)",
    )
    users = parser.add_mutually_exclusive_group()
    users.add_argument(
        "-u",
        "--user",
        action="store",
        type=str,
        default=os.environ["USER"],
        help="The user that will own the written files. Can't be used with '-U'.",
    )
    users.add_argument(
        "-U",
        "--uid",
        action="store",
        type=int,
        default=pwd.getpwnam(os.environ["USER"]).pw_uid,
        help="The uid of the user that will own the written files. Can't be used with '-u'.",
    )
    groups = parser.add_mutually_exclusive_group()
    groups.add_argument(
        "-g",
        "--group",
        action="store",
        type=str,
        default="media-server",
        help="The group that will own the written files. Can't be used with '-G'.",
    )
    groups.add_argument(
        "-G",
        "--gid",
        action="store",
        type=int,
        default=1006,
        help="The gid of the group that will own the written files. Can't be used with '-g'.",
    )
    parser.parse_args(namespace=CLI_STATE)
    if len(CLI_STATE.FILES) == 0:
        CLI_STATE.FILES.append(p.Path("."))
    CLI_STATE.FILES = [file.absolute() for file in CLI_STATE.FILES]

    if CLI_STATE.user != os.environ["USER"]:
        CLI_STATE.uid = pwd.getpwnam(CLI_STATE.user).pw_uid
    if CLI_STATE.group != os.environ["USER"]:  # TODO: This is not the default used!
        CLI_STATE.gid = grp.getgrnam(CLI_STATE.group).gr_gid
    return parser


if __name__ == "__main__":

    def handler(signum: int, frame: object) -> None:
        out = sys.stdout
        signame = signal.Signals(signum).name
        out.write("Received {} ({})\n".format(signame, signum))
        print_at_finish()
        sys.exit(2)

    def print_at_finish() -> None:
        global TOTAL_SAVED
        global TIME_START
        global CURRENT_CONVERT
        global CURRENT_OBJECT
        out = sys.stdout
        total_time_taken = datetime.datetime.now() - TIME_START

        ## Maybe instead of 'possible', verify if the file is complete
        # and if not, delete automatically (except if -f/--keep-failures)
        if not CURRENT_CONVERT.is_dir() and CURRENT_CONVERT.exists():
            os.remove(CURRENT_CONVERT)
        print_to_width("=" * shutil.get_terminal_size().columns, init_gap="")
        out.write(
            write_to_width(
                "Total (partial?) Savings:",
                str(convert_bytes(TOTAL_SAVED)),
                "(" + str(TOTAL_SAVED) + ")",
            )
        )
        out.write("\n")
        out.write(
            write_to_width(
                "Total (partial?) Time:",
                write_timedelta(total_time_taken),
                "(" + str(total_time_taken) + ")",
            )
        )
        if VERIFIED_TRANSCODES:
            print_to_width("Okay to delete:")
            for name in VERIFIED_TRANSCODES:
                print_to_width(name)
        out.flush()
        signal_notify("❌ Catastrophic Error!")

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGPIPE, handler)
    setup_argparse()
    if not TURN_OFF_MAIN:
        try:
            main()
        except Exception:
            exc_info = sys.exc_info()
            exc = "".join(
                traceback.format_exception_only(*(sys.exc_info()[:-1]))
            ).strip()
            tb_lines = traceback.format_tb(sys.exc_info()[-1])
            tb_lines = [
                line
                for split_line in [line.split("\n") for line in tb_lines]
                for line in split_line
            ]
            tb_lines = tb_lines[:-1] if tb_lines[-1].strip() == "" else tb_lines

            gap_width = 4
            start_space_re = "^\\s*"
            print_to_width("=" * shutil.get_terminal_size().columns, init_gap="")
            print("Main Loop Failed")
            sep = "~" * len(exc.strip())
            print_to_width(sep)
            print_to_width(exc)
            print()
            for line in tb_lines:
                mayb_start_space = re.match(start_space_re, line)
                start_space = (
                    "" if mayb_start_space is None else mayb_start_space.group(0)
                )
                print_to_width("{}{}".format(start_space, line))
            print_to_width(sep)
            sys.exit(10)

        if VERIFIED_TRANSCODES:
            print_to_width("~~~~~~~~~~~~~~~")
            print_to_width("Okay to delete:")
            for name in VERIFIED_TRANSCODES:
                print_to_width(name)


def test_print_d_colors() -> None:
    global CLI_STATE
    CLI_STATE.verbosity = 4
    print_d(
        "hello "
        + color_text(CHECK, TermColor.Green)
        + " something a bit longer "
        + color_text(
            "what is something "
            + color_text("about", TermColor.Yellow)
            + " the watchers",
            TermColor.Magenta,
        )
        + " bud"
        + " in this cruel world of ours, sometimes it is necessary",
        bold=True,
        inv=True,
        color=TermColor.Red,
    )


def test_print_d_mostly_plain() -> None:
    global CLI_STATE
    CLI_STATE.verbosity = 4
    print_d(
        "hello "
        + " something a bit longer "
        + "what is something "
        + color_text(
            color_text("about", TermColor.Yellow) + " the watchers",
            TermColor.Magenta,
        )
        + " bud"
        + " in this cruel world of ours, sometimes it is necessary",
        color=TermColor.White,
    )
