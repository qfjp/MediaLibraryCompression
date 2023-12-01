#!/bin/python
import _io  # type: ignore

import argparse as ap
import datetime
import grp
import humanize
import json
import math
import os
import pathlib as p
import pwd
import re
import shutil
import subprocess as s
import sys

from enum import Enum, unique, auto
from textwrap import TextWrapper
from typing import Callable, Optional, ParamSpec, TypeVar
from functools import reduce

import ffpb  # type: ignore

os.nice(10)


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
    keep_failures: bool
    keep_original: bool
    dry_run: bool
    num_files: int
    encoder: str


class UMask:
    def __init__(self, string: str):
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
MPEG4_EXTS = set([".m4v", ".mp4"])
# OLD OLD Extensions, questionable quality
QUESTIONABLE_EXTS = set([".avi", ".mpg"])

COLLISION_SUFFIX = "converted"
PRINT_WIDTH = 68
CLI_STATE = CliState()
DEBUG_STREAM = sys.stdout
D_STRING = "--> "

# Preserve AV1/VP9 encoded videos for now
SKIP_CODECS = ["AV1", "VP9"]
# Leave video codecs as None, this will get replaced by the argument
# parser
CODEC_ID_MAP = {
    # Video
    # "V_AV1": AV1_ENCODER,
    "V_MPEG4/ISO/AVC": None,
    "V_MPEGH/ISO/HEVC": None,
    "avc1": None,
    "XVID": None,
    "27": None,  # Bluray for "x264"
    # Audio
    "A_EAC3": "copy",
    "A_DTS": "copy",
    "A_AC3": "copy",
    "ac-3": "copy",
    "2000": "copy",  # Found for AC-3 in an avi/xvid file
    "131": "copy",  # Bluray for AC-3
    "129": "copy",  # Bluray for AC-3
    "A_TRUEHD": "copy",
    "AAC": "copy",
    "A_AAC-2": "copy",
    "mp4a-40-2": "copy",
    "A_MPEG/L3": "copy",
    "A_OPUS": "copy",
    # Subtitle
    "S_HDMV/PGS": "dvdsub",
    "144": "dvdsub",  # Bluray for PGS
    "S_VOBSUB": "dvdsub",
    "S_TEXT/ASS": "mov_text",
    "S_TEXT/UTF8": "mov_text",
    "tx3g": "mov_text",
}


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


def exact_match(x: str, y: str) -> bool:
    return x == y


def exact_int(x: str, y: str) -> bool:
    return safer_int(x) == safer_int(y)


def set_match(x: str, y: str) -> bool:
    return string_val_to_set(x) == string_val_to_set(y)


def almost_int(x: str, y: str) -> bool:
    return math.fabs(safer_int(x) - safer_int(y)) <= 1


def fuzzy_int(x: str, y: str) -> bool:
    return math.fabs(safer_int(x) - safer_int(y)) <= INT_TOLERANCE


def fuzzy_float(x: str, y: str) -> bool:
    return math.fabs(safer_float(x) - safer_float(y)) <= FLOAT_TOLERANCE


def fuzziest_float(x: str, y: str) -> bool:
    return math.fabs(safer_float(x) - safer_float(y)) <= FUZZIEST_TOLERANCE


def matches(match_str: str) -> Callable[[str, str], bool]:
    return lambda _, y: y == match_str


INT_TOLERANCE = 6
FLOAT_TOLERANCE = 0.2
CROSS = "✖"
CHECK = "✔"
GAP = "    "

PS = ParamSpec("PS")
R = TypeVar("R")

GEN_CACHE: dict[str, dict[PS, R]] = dict()


class ExtraClassProperty:
    def __init__(self, *args: PS.args, **kwargs: PS.kwargs):
        self.prop_list = args + tuple((key, kwargs[key]) for key in kwargs)


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
    def __gt__(self, other):  # type: ignore
        return self.name > other.name

    def __lt__(self, other):  # type: ignore
        return self.name < other.name

    def __eq__(self, other):  # type: ignore
        return self.name == other.name

    def __hash__(self):  # type: ignore
        return self.name.__hash__()

    def get_valid_types(self) -> set[StreamType]:
        return self.value[0]  # type: ignore

    def get_comparison_func(self) -> Callable[[str, str], bool]:
        return self.value[1]  # type: ignore

    ## These tests tend to fail:
    # FrameCount = (set([StreamType.General, StreamType.Video]), almost_int)
    # FrameRate_Den = (set([StreamType.Video, StreamType.Audio]), exact_int)
    # FrameRate_Mode = (set([StreamType.Video]), exact_match)
    # FrameRate_Num = (set([StreamType.Video, StreamType.Audio]), exact_int)
    # BitRate_Maximum = (set([StreamType.Audio]), exact_int)

    # These are solely for categorization, so the comparison
    # function is const(True)
    Format = (
        set([StreamType.General, StreamType.Video, StreamType.Audio, StreamType.Text]),
        lambda x, y: True,
        auto(),
    )
    CodecID = (
        set([StreamType.Video, StreamType.Audio, StreamType.Text]),
        lambda x, y: True,
        auto(),
    )
    StreamOrder = (
        set([StreamType.Video, StreamType.Audio, StreamType.Text]),
        lambda x, y: True,
        auto(),
    )

    Duration = (
        set([StreamType.General, StreamType.Video, StreamType.Audio]),
        fuzziest_float,
        auto(),
    )
    FrameRate = (
        set([StreamType.General, StreamType.Video, StreamType.Audio]),
        fuzzy_float,
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
    ScanType = (set([StreamType.Video]), matches("Progressive"), auto())
    Width = (set([StreamType.Video]), exact_int, auto())
    ChannelLayout = (set([StreamType.Audio]), set_match, auto())
    ChannelPositions = (set([StreamType.Audio]), set_match, auto())
    Channels = (set([StreamType.Audio]), exact_int, auto())
    Compression_Mode = (set([StreamType.Audio]), exact_match, auto())
    SamplingRate = (set([StreamType.Audio]), exact_int, auto())
    ServiceKind = (set([StreamType.Audio]), exact_match, auto())


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
        kwarg_pairs = tuple([(key, kwargs[key]) for key in kwargs])
        try:
            return GEN_CACHE[func_name][arg_tuple + kwarg_pairs]
        except KeyError:
            pass
        dict_args = tuple([f"{key}{kwargs[key]}" for key in kwargs])
        print_d(
            f"{D_STRING}Caching result for {func_name}{args + dict_args}",
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


@cache
def mediainfo(
    path: p.Path, typ: Optional[StreamType] = None
) -> list[dict[StreamProperty, str]]:
    """
    Serialize the output of :code:`mediainfo --output=JSON ...`.

    :param p.Path path: The file path to pass as an argument to :func:`mediainfo`
    :param StreamType typ: The
    :returns:
    :rtype: list[dict[StreamProperty, str]]
    """

    if type(path) != p.PosixPath:
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

    typ_json_dicts = list(filter(lambda obj: obj["@type"] == typ.name, file_json)) # type: ignore
    try:
        typ_json_dicts = list(
            filter(lambda obj: float(obj["Duration"]) != 0, typ_json_dicts)
        )
    except KeyError:
        pass
    if typ == StreamType.General:
        if len(typ_json_dicts) != 1:
            raise AssertionError(f"Number of '{typ}' objects in '{path}' is not one")

    valid_prop_names = list(
        map(
            lambda enm: enm.name,
            filter(lambda enm: typ in enm.get_valid_types(), list(StreamProperty)),
        )
    )

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
    string: str,
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
    wrapper = TextWrapper(
        initial_indent=init_gap,
        break_long_words=False,
        width=width,
        subsequent_indent=subs_gap,
    )
    return delim.join(
        list(map(lambda x: delim.join(wrapper.wrap(x)), string.splitlines(True)))
    )


def print_to_width(
    string: str,
    init_gap: str = GAP,
    subs_gap: str = GAP,
    delim: str = "\n",
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
    :param _io.TextIOWrapper outstream: The file stream to write to
      (stdout by default).
    :returns: None
    """
    outstream = sys.stdout
    outstream.write(
        write_to_width(
            string,
            init_gap=init_gap,
            subs_gap=subs_gap,
            delim=delim,
        )
    )
    outstream.write("\n")


def print_d(
    *args: PS.args,
    verbosity_limit: int = 2,
    color: TermColor = TermColor.Blue,
    inv: bool = False,
    bold: bool = False,
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
    :type outstream: :class:`_io.TextIOWrapper`
    :returns: None
    """
    outstream = sys.stdout
    if verbosity_limit > CLI_STATE.verbosity:
        return
    d_str = f"{GAP}  ---> "
    blank = " " * (len(d_str) - 3) + "-> "
    result = write_to_width(
        " ".join(map(str, args)), init_gap=d_str, subs_gap=blank, delim="\n"
    )
    color_val = 8
    if color is not None:
        color_val = color.value
    color_prefix = 4 if inv else 3
    bold_code = 1 if bold else 0
    escape_code = f"[{bold_code};{color_prefix}{color_val}m"
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
    outstream.write("\n")


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
        key_pairs = list(map(lambda key_pair: key_pair.split(":"), by_comma))
        return reduce(lambda x, y: x.union(y), [set(key_val) for key_val in key_pairs])
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
    color_val = color.value
    color_prefix = 4 if inv else 3
    bold_code = 1 if bold else 0
    escape_code = f"[{bold_code};{color_prefix}{color_val}m"
    return_code = "[0m"
    # Replace all end escape codes (^[[0m) in the original string
    # with a new color following them (^[[0m^[[...), unless they
    # end the string or are already followed by a color
    colors_preserved = re.sub("\[0m(?!|$)", f"[0m{escape_code}", string)
    return escape_code + colors_preserved + return_code


@cache
def get_pairs(
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
    f1 = mediainfo(fname1, typ)
    f2 = mediainfo(fname2, typ)
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
    compare_func: Callable[[str, str], bool] = exact_match,
) -> bool:
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
        olds = list(map(lambda x: x[0][key], pairs))
    except KeyError:
        olds = []
    try:
        news = list(map(lambda x: x[1][key], pairs))
    except KeyError:
        news = []
    try:

        def possible_val(ix: int, lst: list[str]) -> str:
            return "" if len(lst) <= ix else lst[ix]

        results = [
            (
                compare_func(possible_val(ix, olds), possible_val(ix, news)),
                possible_val(ix, olds),
                possible_val(ix, news),
            )
            for ix in range(max(len(olds), len(news)))
        ]
    except AttributeError:
        return False
    except TypeError:
        return False
    return all(map(lambda tup: tup[0], results))


@cache
def verify_conversion_tests(
    fname1: p.Path, fname2: p.Path
) -> list[tuple[bool, StreamType, StreamProperty]]:
    """
    Verify the conversion of a media file.

    :param p.Path fname1: The original media file
    :param p.Path fname2: The converted media file
    :returns: A list of tests and their results, in the form of a
      tuple as (`result`, `stream type`, `mediainfo json key`)
    :rtype: list[tuple[bool, StreamType, str]]
    """
    if not (fname1.exists() and fname2.exists()):
        raise IOError("Comparison can't proceed because file doesn't exist.")

    tests = list(StreamProperty)
    if any(
        map(
            lambda json: json[StreamProperty.Format] == "Opus",
            mediainfo(fname2, StreamType.Audio),
        )
    ):
        tests = list(
            filter(
                lambda prop: not (
                    StreamType.Audio in prop.get_valid_types()
                    and prop.name
                    in ["ChannelLayout", "ChannelPositions", "Channels", "FrameRate"]
                ),
                tests,
            )
        )

    results = list(
        map(
            lambda prop: list(
                map(
                    lambda typ: (
                        compare_stream_property(
                            get_pairs(fname1, fname2, typ),
                            prop,
                            prop.get_comparison_func(),
                        ),
                        typ,
                        prop,
                    ),
                    prop.get_valid_types(),
                )
            ),
            tests,
        )
    )
    orig_results = []
    for comparison_lst in results:
        for comparison in comparison_lst:
            orig_results.append(comparison)
    return orig_results


def verify_conversion(fname1: p.Path, fname2: p.Path) -> bool:
    """
    Verify that the two files match, up to the tests in :code:`STREAM_PROP_TESTS`.

    :returns: True if :code:`fname1` ~= :code:`fname2`
    """
    results = verify_conversion_tests(fname1, fname2)

    return all(map(lambda tup: tup[0], results))


def failed_tests(fname1: p.Path, fname2: p.Path) -> str:
    """
    Return a 'prettified' string detailing the tests that :code:`fname1`
      and :code:`fname2` do not match on.
    """
    results = verify_conversion_tests(fname1, fname2)
    result_tups: list[tuple[StreamType, StreamProperty]] = list(
        map(lambda tup: tup[1:], filter(lambda x: not x[0], results))
    )
    pretty_results = "\n".join(
        map(
            lambda tup: str(tup[0]) + ": " + color_text(tup[1].name, TermColor.Red),
            result_tups,
        )
    )
    return pretty_results


def get_converted_name(path: p.Path) -> p.Path:
    """
    Give the name used for the new file in the conversion of :code:`path`
    """
    if path.suffix not in MPEG4_EXTS:
        return path.with_suffix(".mp4")
    else:
        return path.with_suffix(f".{COLLISION_SUFFIX}{path.suffix}")


def is_skip_codec(path: p.PosixPath) -> bool:
    """
    Whether the given file should not be converted, on the basis of its :code:`Format`.
    """
    vid_jsons = mediainfo(path, StreamType.Video)
    codecs = list(map(lambda obj: obj[StreamProperty.Format], vid_jsons))
    return any(map(lambda codec: codec in SKIP_CODECS, codecs))


def is_mp4_x265(path: p.PosixPath) -> bool:
    """
    Whether the given file is already HEVC in an MPEG-4 container.
    """
    gen_json = mediainfo(path, StreamType.General)[0]
    container = None
    try:
        container = gen_json[StreamProperty.Format]
    except KeyError:
        raise (ValueError(f"File {path} doesn't look like a video file"))
    if container != "MPEG-4":
        return False
    vid_jsons = mediainfo(path, StreamType.Video)
    codecs = list(map(lambda obj: obj[StreamProperty.Format], vid_jsons))
    codec_ids = list(map(lambda obj: obj[StreamProperty.CodecID], vid_jsons))

    return all(map(lambda codec: codec == "HEVC", codecs)) and all(
        map(lambda codec_id: codec_id == "hvc1" or codec_id == "hev1", codec_ids)
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
            init_gap=GAP,
            subs_gap=2 * GAP,
        )
        raise RuntimeError(err)

    ffprobe_stdout = list(
        filter(
            lambda packet_lst: len(packet_lst) == 3,
            list(
                map(
                    lambda packet_str: packet_str.split(","),
                    ffprobe_out.stdout.decode("utf-8").split("\n"),
                )
            ),
        )
    )
    all_stream_ixs = set(map(lambda packet_lst: int(packet_lst[1]), ffprobe_stdout))
    invalid_ixs = set(
        map(
            lambda packet_lst: int(packet_lst[1]),
            filter(lambda packet_lst: packet_lst[2] == "N/A", ffprobe_stdout),
        )
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
    codec_ids = list(
        map(lambda typ_json: typ_json[StreamProperty.CodecID], mediainfo(path, typ))
    )

    if validate:
        all_stream_ixs, invalid_ixs = validate_streams_to_convert(
            path, typ, num_packets=num_frames
        )
    else:
        invalid_ixs = set()
        all_stream_ixs = set(
            map(
                lambda sub_json: int(
                    clean_stream_order(sub_json[StreamProperty.StreamOrder])
                ),
                mediainfo(path, typ),
            )
        )

    # If 200 packets is too small, keep tryin'
    more_frames = False
    while validate and len(all_stream_ixs) != len(mediainfo(path, typ)):
        num_frames *= 2
        if not more_frames:
            print_to_width(
                f"Calculated stream indexes ({len(all_stream_ixs)}) for type '{typ}' do not match the mediainfo output({len(mediainfo(path, typ))})",
                init_gap=2 * GAP,
                subs_gap=2 * GAP,
            )
        sys.stdout.write(
            write_to_width(
                f"Searching {num_frames} for streams -> ",
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
        map(
            lambda sub_json: int(
                clean_stream_order(sub_json[StreamProperty.StreamOrder])
            ),
            mediainfo(path, typ),
        )
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
    print_d("valid, invalid:", valid_stream_ixs, invalid_ixs, verbosity_limit=3)

    num_codecs_so_far = 0
    encoding_args = []
    try:
        offset = min(all_stream_ixs)
    except ValueError:
        offset = 0
    for num_codecs_so_far, stream_ix in enumerate(valid_stream_ixs):
        type_ix = stream_ix - offset
        codec_id = codec_ids[type_ix]
        encoding = None
        encoding = CODEC_ID_MAP[codec_id]

        encode_key = typ.ffprobe_ident()
        encoding_args += [
            "-map",
            f"0:{encode_key}:{type_ix}",
            f"-c:{encode_key}:{num_codecs_so_far}",
            f"{encoding}",
        ]
    return encoding_args


def pprint_ffmpeg(path: p.Path) -> str:
    """
    Pretty print the ffmpeg command that will be used to convert the
    given file.
    """
    lst = ffmpeg_cmd(path).copy()
    ix = lst.index(str(path))
    lst[ix] = '"' + lst[ix] + '"'
    lst[-1] = '"' + lst[-1] + '"'
    width = shutil.get_terminal_size().columns
    delim = " \\ \n"

    return delim.join(
        [
            write_to_width(" ".join(lst[0:ix]), delim=delim),
            2 * GAP + f"{lst[ix]}",
            write_to_width(
                " ".join(lst[ix + 1 : -1]),
                init_gap=2 * GAP,
                subs_gap=2 * GAP,
                delim=delim,
            ),
            2 * GAP + f"{lst[-1]}",
        ]
    )


@cache
def ffmpeg_cmd(path: p.Path) -> list[str]:
    video_tracks = mediainfo(path, StreamType.Video)
    heights = set(map(lambda track: track[StreamProperty.Height], video_tracks))
    widths = set(map(lambda track: track[StreamProperty.Width], video_tracks))

    # assert len(heights) == 1 and len(widths) == 1

    # TODO: These may not be the same index
    height = max(heights)
    width = max(widths)

    # Without a duration field, validation will fail
    subtitles_have_duration = True
    subtitle_conversions = []
    try:
        list(
            map(
                lambda track: track[StreamProperty.Duration],
                mediainfo(path, StreamType.Text),
            )
        )
    except KeyError:
        subtitles_have_duration = False
    if subtitles_have_duration:
        subtitle_conversions = generate_conversions(
            path, StreamType.Text, validate=True
        )
    else:
        subtitle_conversions = generate_conversions(path, StreamType.Text)

    cmd = (
        [
            "ffmpeg",
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
        + generate_conversions(path, StreamType.Video)
        + generate_conversions(path, StreamType.Audio)
        + subtitle_conversions
        + [str(get_converted_name(path))]
    )
    return cmd


def process_vidlist(
    vidlist: [p.PosixPath],
    limit: int = None,
    convert_and_replace: bool = True,
    keep_failures: bool = False,
    just_list: bool = False,
    dry_run: bool = False,
) -> float:
    total_size_saved = 0
    limit = len(vidlist) if (limit is None) else min(len(vidlist), limit)
    limit_digits = len(f"{limit}")

    num_processed = 0
    for cur_path in vidlist:
        # Because the size of the vidlist is iffy
        if num_processed >= limit:
            return total_size_saved

        if is_mp4_x265(cur_path):
            continue
        if just_list:
            print(os.path.getsize(cur_path), cur_path)
            continue

        new_path = get_converted_name(cur_path)
        format_string = "({:" + f"{limit_digits}d" + "}/{:d}) Processing {}"

        print_to_width(
            format_string.format(num_processed + 1, limit, cur_path.name),
            init_gap="",
            subs_gap=GAP,
        )
        print_to_width(
            f"Dir: {cur_path.parent}",
            subs_gap=GAP + "".join(map(lambda x: " ", "Dir: ")),
        )
        print_to_width(
            f"New file: {new_path}",
            subs_gap=GAP + "".join(map(lambda x: " ", "New file: ")),
            delim="\\ \n",
        )
        print()
        if is_skip_codec(cur_path):
            print_to_width(color_text("AV1/VP9 file!", TermColor.Green))
            print()
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
            if not verified:
                print_to_width(
                    failed_tests(cur_path, new_path),
                    init_gap=GAP + "  ",
                    subs_gap=2 * GAP,
                    delim=",\n",
                )
            print()
            continue
        ffmpeg_proc_str = ffmpeg_cmd(cur_path)

        print(pprint_ffmpeg(cur_path))
        print()

        if dry_run:
            continue

        before = datetime.datetime.now()
        ffpb.main(argv=ffmpeg_proc_str[1:], stream=sys.stderr)
        after = datetime.datetime.now()
        time_taken = after - before
        print_to_width(f"Conversion Runtime: {write_timedelta(time_taken)}")
        set_fprops(new_path)

        if verify_conversion(cur_path, new_path):
            old_size = os.path.getsize(cur_path)
            new_size = os.path.getsize(new_path)
            diff_size = old_size - new_size
            total_size_saved += diff_size
            compression = find_compression_ratio(cur_path, new_path)
            print_to_width(f"{color_text(CHECK, TermColor.Green)} Metadata matches")
            print_to_width(
                f"Compression: {compression:.3f}%", init_gap=(2 * GAP + "  ")
            )
            print_to_width(
                f"Savings: {humanize.naturalsize(diff_size)} = {humanize.naturalsize(old_size)} - {humanize.naturalsize(new_size)}",
                init_gap=2 * GAP + "  ",
            )
            if diff_size < 0:
                print_to_width(color_text("No savings!", TermColor.Red))
                print_to_width(f"Keeping {color_text(str(new_path), TermColor.Red)}")
                num_processed += 1
                continue
            if cur_path.suffix in QUESTIONABLE_EXTS:
                print_to_width(color_text("Weird Extension", TermColor.Magenta))
                print_to_width(
                    f"Keeping {color_text(str(new_path), TermColor.Magenta)}"
                )
                num_processed += 1
                continue

            if convert_and_replace and cur_path.suffix == ".mp4":
                os.replace(new_path, cur_path)
            elif convert_and_replace:
                os.remove(cur_path)
            elif cur_path.suffix == ".mp4":
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
            print_to_width(f"{color_text(CROSS, TermColor.Red)} Metadata mismatch")
            print_to_width(
                failed_tests(cur_path, new_path),
                init_gap=GAP + "  ",
                subs_gap=2 * GAP,
                delim=",\n",
            )
            if not keep_failures:
                os.remove(new_path)
                print()
            else:
                print_to_width(f"Keeping {color_text(str(new_path), TermColor.Red)}")
                print()
                num_processed += 1
            continue

        num_processed += 1
        print()
    # Because the size of the vidlist is accurate
    return total_size_saved


def human_readable(num: float, suffix: str = "B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def find_compression_ratio(f1: p.Path, f2: p.Path) -> float:
    f1_size = os.path.getsize(f1)
    f2_size = os.path.getsize(f2)
    return 100 * f2_size / f1_size


def main() -> None:
    # Retrieve the program state from arguments
    just_list = CLI_STATE.list_files
    keep_failures = CLI_STATE.keep_failures
    replace = not CLI_STATE.keep_original
    dry_run = CLI_STATE.dry_run
    num_files = CLI_STATE.num_files
    for key, val in CODEC_ID_MAP.items():
        if val is None:
            CODEC_ID_MAP[key] = CLI_STATE.encoder

    # Process given files
    files = list(filter(lambda x: x.is_file(), CLI_STATE.FILES))
    # Check if the files are media files
    nonmedia_files = set(
        filter(lambda x: len(VIDEO_FILE_EXTS.intersection(set(x.suffixes))) == 0, files)
    )
    if nonmedia_files:
        raise (
            ValueError(
                f"The files {nonmedia_files} can not be converted (must have ext in {VIDEO_FILE_EXTS})."
            )
        )

    # Process given folders
    folders = list(filter(lambda x: x.is_dir(), CLI_STATE.FILES))

    def glob_folder(path: p.Path) -> list[p.Path]:
        return reduce(lambda x, y: x + y, map(lambda ext: list(path.glob(f"**/*{ext}")), VIDEO_FILE_EXTS))

    globbed = list(map(glob_folder, folders))
    accumulated_globs = []
    if globbed:
        accumulated_globs = reduce(lambda x, y: x + y, globbed)
    vid_list = list(files) + accumulated_globs
    assert len(files) + len(folders) == len(CLI_STATE.FILES)

    # Start the clock
    orig_time = datetime.datetime.now()

    files_and_sizes: list[tuple[p.Path, int]] = list(map(lambda file: (file, os.path.getsize(file)), vid_list))
    files_and_sizes = sorted(files_and_sizes, key=lambda video_size: -video_size[1])

    videos = list(map(lambda video_size_tup: video_size_tup[0], files_and_sizes))
    total_size_saved = process_vidlist(
        videos,
        limit=num_files,
        convert_and_replace=replace,
        just_list=just_list,
        keep_failures=keep_failures,
        dry_run=dry_run,
    )
    total_time_taken = datetime.datetime.now() - orig_time

    if just_list:
        print_d(GEN_CACHE, verbosity_limit=4)
        return

    print(
        "Total Savings:",
        str(humanize.naturalsize(total_size_saved)),
        "(" + str(total_size_saved) + ")",
    )
    print(
        "Total Time:",
        write_timedelta(total_time_taken),
        "(" + str(total_time_taken) + ")",
    )


if __name__ == "__main__":

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
        "-e",
        "--encoder",
        choices=["libx265", "hevc_nvenc", "libsvt_hevc"],
        default="libx265",
        help="The video encoder to use.",
    )
    parser.add_argument(
        "-o",
        "--keep-original",
        action="store_true",
        help="If set, don't remove the original file after encoding.",
    )
    parser.add_argument(
        "-k",
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

    if CLI_STATE.user != os.environ["USER"]:
        CLI_STATE.uid = pwd.getpwnam(CLI_STATE.user).pw_uid
    if CLI_STATE.group != os.environ["USER"]:  # TODO: This is not the default used!
        CLI_STATE.gid = grp.getgrnam(CLI_STATE.group).gr_gid

    main()


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
        color=None,
    )
