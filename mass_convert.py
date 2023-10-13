#!/bin/python
import _io

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

from enum import Enum, unique
from textwrap import TextWrapper
from typing import Callable
from functools import reduce

import ffpb

os.nice(10)


class CliState:
    pass


class UMask:
    def __init__(self, string: str):
        """
        :param string: An octal umask (string of 3 or 4 numbers, from 0 to 7).
        """
        if len(string) not in [3, 4]:
            raise ValueError("UMask must be a string of 3 or 4 numbers")
        overall_mask = 0
        for ix, digit in enumerate(string[:0:-1]):
            digit = int(digit)
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

    def max_streams(self):
        """
        The maximum number of streams of a given type that `should` be
        encountered in a typical output of :func:`mediainfo`. This is
        used as a sanity check.
        """
        if self == StreamType.General:
            return 1
        else:
            return None

    def ffprobe_ident(self):
        """
        The identifier for a given stream type used by :code:`ffprobe`.
        """
        if self == StreamType.Text:
            return "s"
        return self.name[0].lower()


# NVENC_SETTINGS = ["-tune", "hq", "-rc", "vbr", "-multipass", "qres", "-b_ref_mode", "each", "-rc-lookahead", 32, "-strict_gop", "1", "-spatial_aq", "1", "temporal_aq", "1"]
# AV1_ENCODER = "libsvtav1"

# Extensions of convertable files
VIDEO_FILE_EXTS = set([".avi", ".mpg", ".mkv", ".webm", ".m4v", ".mp4"])
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
    # Audio
    "A_EAC3": "copy",
    "A_DTS": "copy",
    "A_AC3": "copy",
    "ac-3": "copy",
    "2000": "copy",  # Found for AC-3 in an avi/xvid file
    "A_TRUEHD": "copy",
    "AAC": "copy",
    "A_AAC-2": "copy",
    "mp4a-40-2": "copy",
    "A_MPEG/L3": "copy",
    "A_OPUS": "copy",
    # Subtitle
    "S_HDMV/PGS": "dvdsub",
    "S_VOBSUB": "dvdsub",
    "S_TEXT/ASS": "mov_text",
    "S_TEXT/UTF8": "mov_text",
    "tx3g": "mov_text",
}

exact_match = lambda x, y: x == y
exact_int = lambda x, y: int(x) == int(y)
set_match = lambda x, y: string_val_to_set(x) == string_val_to_set(y)
almost_int = lambda x, y: math.fabs(int(x) - int(y)) <= 1
fuzzy_int = lambda x, y: math.fabs(int(x) - int(y)) <= INT_TOLERANCE
fuzzy_float = lambda x, y: math.fabs(float(x) - float(y)) <= FLOAT_TOLERANCE
fuzziest_float = lambda x, y: math.fabs(float(x) - float(y)) <= INT_TOLERANCE
matches = lambda match_str: lambda _, y: y == match_str

STREAM_PROP_COMPARE_FUNCS = {
    (StreamType.General, "AudioCount"): exact_int,
    (StreamType.General, "Duration"): fuzziest_float,
    (StreamType.General, "FrameCount"): almost_int,
    (StreamType.General, "FrameRate"): fuzzy_float,
    (StreamType.General, "IsStreamable"): matches("Yes"),
    (StreamType.General, "Title"): exact_match,
    (StreamType.General, "VideoCount"): exact_int,
    # Video
    (StreamType.Video, "BitDepth"): exact_match,
    (StreamType.Video, "ChromaSubsampling"): exact_match,
    (StreamType.Video, "ColorSpace"): exact_match,
    (StreamType.Video, "DisplayAspectRatio"): fuzzy_float,
    (StreamType.Video, "Duration"): fuzziest_float,
    (StreamType.Video, "FrameCount"): almost_int,
    (StreamType.Video, "FrameRate"): fuzzy_float,
    (StreamType.Video, "FrameRate_Den"): exact_int,
    (StreamType.Video, "FrameRate_Mode"): exact_match,
    (StreamType.Video, "FrameRate_Num"): exact_int,
    (StreamType.Video, "Height"): exact_int,
    (StreamType.Video, "PixelAspectRatio"): exact_match,
    (StreamType.Video, "Sampled_Height"): exact_int,
    (StreamType.Video, "Sampled_Width"): exact_int,
    (StreamType.Video, "ScanType"): matches("Progressive"),
    (StreamType.Video, "Width"): exact_int,
    # Audio
    (StreamType.Audio, "BitRate_Maximum"): exact_int,
    (StreamType.Audio, "ChannelLayout"): set_match,
    (StreamType.Audio, "ChannelPositions"): set_match,
    (StreamType.Audio, "Channels"): exact_int,
    (StreamType.Audio, "Compression_Mode"): exact_match,
    (StreamType.Audio, "Duration"): fuzziest_float,
    (StreamType.Audio, "FrameRate"): fuzzy_float,
    (StreamType.Audio, "FrameRate_Den"): exact_int,
    (StreamType.Audio, "FrameRate_Num"): exact_int,
    (StreamType.Audio, "SamplingRate"): exact_int,
    (StreamType.Audio, "ServiceKind"): exact_match,
}

STREAM_PROP_TESTS = [
    # General
    (StreamType.General, "AudioCount"),  # exact_int
    (StreamType.General, "Duration"),  # fuzziest_float
    # (StreamType.General, "FrameCount"),  # almost_int
    (StreamType.General, "FrameRate"),  # fuzzy_float
    (StreamType.General, "IsStreamable"),  # matches("Yes")
    (StreamType.General, "Title"),
    (StreamType.General, "VideoCount"),  # exact_int
    # Video
    (StreamType.Video, "BitDepth"),
    (StreamType.Video, "ChromaSubsampling"),
    (StreamType.Video, "ColorSpace"),
    (StreamType.Video, "DisplayAspectRatio"),  # fuzzy_float
    (StreamType.Video, "Duration"),  # fuzziest_float
    # (StreamType.Video, "FrameCount"),  # almost_int
    (StreamType.Video, "FrameRate"),  # fuzzy_float
    # (StreamType.Video, "FrameRate_Den"),  # exact_int
    # (StreamType.Video, "FrameRate_Mode"),
    # (StreamType.Video, "FrameRate_Num"),  # exact_int
    (StreamType.Video, "Height"),  # exact_int
    (StreamType.Video, "PixelAspectRatio"),
    (StreamType.Video, "Sampled_Height"),  # exact_int
    (StreamType.Video, "Sampled_Width"),  # exact_int
    (StreamType.Video, "ScanType"),  # matches("Progressive")
    (StreamType.Video, "Width"),  # exact_int
    # Audio
    # (StreamType.Audio, "BitRate_Maximum"),  # exact_int
    (StreamType.Audio, "ChannelLayout"),  # set_match
    (StreamType.Audio, "ChannelPositions"),  # set_match
    (StreamType.Audio, "Channels"),  # exact_int
    (StreamType.Audio, "Compression_Mode"),
    (StreamType.Audio, "Duration"),  # fuzziest_float
    (StreamType.Audio, "FrameRate"),  # fuzzy_float
    (StreamType.Audio, "FrameRate_Den"),  # exact_int
    (StreamType.Audio, "FrameRate_Num"),  # exact_int
    (StreamType.Audio, "SamplingRate"),  # exact_int
    (StreamType.Audio, "ServiceKind"),
]

INT_TOLERANCE = 6
FLOAT_TOLERANCE = 0.2
CROSS = "✖"
CHECK = "✔"
GAP = "    "

GEN_CACHE = dict()


class ExtraClassProperty:
    def __init__(self, *args, **kwargs):
        self.prop_list = args + tuple((key, kwargs[key]) for key in kwargs)


@unique
class StreamProperty(Enum):
    """
    A possible `comparable` key in the json object given by
    :code:`mediainfo --output=JSON ...`. The tuple values are the
    actual name found in the JSON object and the possible JSON
    objects that the property might be found in. For example, for the
    property

    .. code-block::

        TypeOrder = _create_val("@typeorder", ["Audio", "Text"])

    this can be found in the output of :code:`mediainfo --output=JSON ...`
    as such:

    .. code-block::

        ...
        { "@type": "Audio",
          "@typeorder": ...
          ...
        },
        { "@type": "Text",
          "@typeorder": ...
        },
        ...

    """

    def valid_streams(self) -> list[StreamType]:
        """
        The valid :py:class:`StreamType`'s that a given property can be
        found in.
        """
        return self.value[1]

    def _create_val(
        value: str, stream_types: list[str]
    ) -> tuple[str, list[StreamType]]:
        return (value, list(map(lambda x: StreamType[x], stream_types)))

    # All
    Type = ("@type", list(StreamType))
    # General, Video, Audio
    UniqueID = _create_val("UniqueID", ["General", "Video", "Audio"])
    Duration = _create_val("Duration", ["General", "Video", "Audio"])
    Format = _create_val("Format", ["General", "Video", "Audio"])
    StreamSize = _create_val("StreamSize", ["General", "Video", "Audio"])
    FrameRate = _create_val("FrameRate", ["General", "Video", "Audio"])
    Title = _create_val("Title", ["General", "Video", "Audio"])
    FrameCount = _create_val("FrameCount", ["General", "Video", "Audio"])
    # Video, Audio, Text
    StreamOrder = _create_val("StreamOrder", ["Video", "Audio", "Text"])
    ID = _create_val("ID", ["Video", "Audio", "Text"])
    CodecID = _create_val("CodecID", ["Video", "Audio", "Text"])
    BitRate = _create_val("BitRate", ["Video", "Audio", "Text"])
    Language = _create_val("Language", ["Video", "Audio", "Text"])
    Default = _create_val("Default", ["Video", "Audio", "Text"])
    Forced = _create_val("Forced", ["Video", "Audio", "Text"])
    # Video, Audio
    BitDepth = _create_val("BitDepth", ["Video", "Audio"])
    BitRate_Mode = _create_val("BitRate_Mode", ["Video", "Audio"])
    Delay = _create_val("Delay", ["Video", "Audio"])
    Delay_Source = _create_val("Delay_Source", ["Video", "Audio"])
    # Audio, Text
    TypeOrder = _create_val("@typeorder", ["Audio", "Text"])
    # General
    VideoCount = _create_val("VideoCount", ["General"])
    AudioCount = _create_val("AudioCount", ["General"])
    TextCount = _create_val("TextCount", ["General"])
    MenuCount = _create_val("MenuCount", ["General"])
    FileExtension = _create_val("FileExtension", ["General"])
    Format_Version = _create_val("Format_Version", ["General"])
    FileSize = _create_val("FileSize", ["General"])
    OverallBitRate_Mode = _create_val("OverallBitRate_Mode", ["General"])
    OverallBitRate = _create_val("OverallBitRate", ["General"])
    IsStreamable = _create_val("IsStreamable", ["General"])
    Movie = _create_val("Movie", ["General"])
    Encoded_Date = _create_val("Encoded_Date", ["General"])
    File_Modified_Date = _create_val("File_Modified_Date", ["General"])
    File_Modified_Date_Local = _create_val("File_Modified_Date_Local", ["General"])
    Encoded_Application = _create_val("Encoded_Application", ["General"])
    Encoded_Library = _create_val("Encoded_Library", ["General"])
    # Video
    Format_Profile = _create_val("Format_Profile", ["Video"])
    Format_Level = _create_val("Format_Level", ["Video"])
    Format_Settings_CABAC = _create_val("Format_Settings_CABAC", ["Video"])
    Format_Settings_RefFrames = _create_val("Format_Settings_RefFrames", ["Video"])
    Format_Settings_GOP = _create_val("Format_Settings_GOP", ["Video"])
    Width = _create_val("Width", ["Video"])
    Height = _create_val("Height", ["Video"])
    Stored_Height = _create_val("Stored_Height", ["Video"])
    Sampled_Width = _create_val("Sampled_Width", ["Video"])
    Sampled_Height = _create_val("Sampled_Height", ["Video"])
    PixelAspectRatio = _create_val("PixelAspectRatio", ["Video"])
    DisplayAspectRatio = _create_val("DisplayAspectRatio", ["Video"])
    FrameRate_Mode = _create_val("FrameRate_Mode", ["Video"])
    FrameRate_Num = _create_val("FrameRate_Num", ["Video"])
    FrameRate_Den = _create_val("FrameRate_Den", ["Video"])
    ColorSpace = _create_val("ColorSpace", ["Video"])
    ChromaSubsampling = _create_val("ChromaSubsampling", ["Video"])
    ScanType = _create_val("ScanType", ["Video"])
    BufferSize = _create_val("BufferSize", ["Video"])
    # Audio
    Format_Commercial_IfAny = _create_val("Format_Commercial_IfAny", ["Audio"])
    Format_Settings_Mode = _create_val("Format_Settings_Mode", ["Audio"])
    Format_Settings_Endianness = _create_val("Format_Settings_Endianness", ["Audio"])
    Format_AdditionalFeatures = _create_val("Format_AdditionalFeatures", ["Audio"])
    Channels = _create_val("Channels", ["Audio"])
    ChannelPositions = _create_val("ChannelPositions", ["Audio"])
    ChannelLayout = _create_val("ChannelLayout", ["Audio"])
    SamplesPerFrame = _create_val("SamplesPerFrame", ["Audio"])
    SamplingRate = _create_val("SamplingRate", ["Audio"])
    SamplingCount = _create_val("SamplingCount", ["Audio"])
    Compression_Mode = _create_val("Compression_Mode", ["Audio"])
    Video_Delay = _create_val("Video_Delay", ["Audio"])
    ServiceKind = _create_val("ServiceKind", ["Audio"])
    # Text
    ElementCount = _create_val("ElementCount", ["Text"])


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
    Grey = 7


def cache(cache_dict=GEN_CACHE):
    """
    A function wrapper that causes a function's results to be memoized

    Results are stored to a default dictionary, but you can supply your
    own cache dictionary if it's necessary to keep separate caches.

    :param dict cache_dict: The structure that acts as the cache. Any
      data structure with the same interface as a dictionary can be
      used (e.g. a splay tree with the proper functions might improve
      performance)
    """

    def inner(func):
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            arg_tuple = args
            kwarg_pairs = tuple([(key, kwargs[key]) for key in kwargs])
            try:
                return cache_dict[func_name][arg_tuple + kwarg_pairs]
            except KeyError:
                pass
            dict_args = tuple([f"{key}{kwargs[key]}" for key in kwargs])
            print_d(
                f"{D_STRING}Caching result for {func_name}{args + dict_args}",
            )
            result = func(*args, **kwargs)
            try:
                cache_dict[func_name][arg_tuple + kwarg_pairs] = result
            except KeyError:
                cache_dict[func_name] = dict()
                cache_dict[func_name][arg_tuple + kwarg_pairs] = result

            return result

        # Doc might be the only necessary thing here
        wrapper.__doc__ = func.__doc__
        wrapper.__annotations__ = func.__annotations__
        wrapper.__name__ = func.__name__
        return wrapper

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

    def reduce_time(multiple, unreduced_in_unit) -> tuple[float, float]:
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
    remain = math.floor(td.total_seconds())
    units = []
    for mult, unit in mults:
        in_unit, remain = reduce_time(mult, remain)
        units.append("{:.2g} {}".format(in_unit, unit))
        if math.fabs(remain) < 1:
            break
    if remain != 0:
        units.append("{:.2g} {}".format(remain, "years"))
    return ", ".join(units[::-1])


def set_fprops(path: p.PosixPath):
    os.chown(path, CLI_STATE.uid, CLI_STATE.gid)
    os.chmod(path, CLI_STATE.umask)


@cache()
def mediainfo(path: p.Path, typ: StreamType = None) -> list[dict[StreamProperty, str]]:
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
    except TypeError as e:
        print(f"========================={path}=================")
        print("Full JSON Found")
        print(mediainfo_out.decode("utf-8"))

        # Force the list to keep building
        fake_out = dict
        fake_out["Format"] = None
        return [fake_out]

    if typ == None:
        return [file_json]

    typ_json = list(filter(lambda obj: obj["@type"] == typ.name, file_json))
    try:
        typ_json = list(filter(lambda obj: float(obj["Duration"]) != 0, typ_json))
    except KeyError:
        pass
    if typ == StreamType.General:
        if len(typ_json) != 1:
            raise AssertionError(f"Number of '{typ}' objects in '{path}' is not one")
    return typ_json


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
    outstream: _io.TextIOWrapper = sys.stdout,
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
    *args,
    verbosity_limit: int = 2,
    color: TermColor = TermColor.Blue,
    inv=False,
    bold=False,
    outstream=sys.stdout,
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
        last_escape_in_line = None
        inner_color = None
        if escapes:
            last_escape_in_line = escapes[-1]
            inner_color = None
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


def string_val_to_set(string) -> set[str]:
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
        return [set(key_val) for key_val in key_pairs]
    return set(string.split(" "))


def color_text(string: str, color: TermColor, inv=False, bold=False) -> str:
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


@cache()
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
    try:
        return [(f1[ix], f2[ix]) for ix in range(len(f1))]
    except IndexError:
        raise TypeError(typ.name)


def get_pair_tests(
    pairs: list[tuple[dict[StreamProperty, str], dict[StreamProperty, str]]]
) -> list[StreamProperty]:
    """
    Convert the output of :func:`get_pairs` to a list of all
      :class:`StreamProperty` values to test

    :param pairs: A list obtained from :func:`get_pairs`, i.e.
      a list of a particular :class:`StreamType`'s
      :func:`mediainfo` output for both :code:`(fname1, fname2)`
    """
    # Convert dicts to keys:
    # [({a: .., b: ..}, {a:, b:}), ({c:, d:}, {..})]
    # => [(a: lst, b: lst), (c, d)]
    result = list(
        map(
            lambda tup: tuple(map(lambda json_dict: list(json_dict), tup)),
            pairs,
        )
    )
    # Reduce all tuples: [(a: lst, b: lst), (c, d)] => [(a + b), (c + d)]
    result = reduce(lambda x, y: x + y, result)
    # Flatten => [a + b + c + d] (and remove duplicates, and sort)
    result = sorted(set(reduce(lambda x, y: x + y, result)))
    return result


def compare_json_key(
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
    if key == "extra":
        return False
    try:
        olds = list(map(lambda x: x[0][key], pairs))
    except KeyError:
        olds = []
    try:
        news = list(map(lambda x: x[1][key], pairs))
    except KeyError:
        news = []
    try:
        old_val = lambda ix: None if len(olds) <= ix else olds[ix]
        new_val = lambda ix: None if len(news) <= ix else news[ix]
        results = [
            compare_func(old_val(ix), new_val(ix))
            for ix in range(max(len(olds), len(news)))
        ]
    except AttributeError:
        return False
    except TypeError:
        return False
    return all(results)


@cache()
def verify_conversion_tests(
    fname1: p.Path, fname2: p.Path
) -> list[tuple[bool, StreamType, str]]:
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

    try:
        general_pairs = get_pairs(fname1, fname2, StreamType.General)
        video_pairs = get_pairs(fname1, fname2, StreamType.Video)
        audio_pairs = get_pairs(fname1, fname2, StreamType.Audio)
    except TypeError as e:
        return [(False, StreamType[str(e)], "JSON length mismatch")]

    # This is not planned to be permanent, except maybe it is
    def all_tests_and_results(all_json_keys, pairs, typ):
        results = []
        for json_key in all_json_keys:
            func = exact_match
            try:
                func = STREAM_PROP_COMPARE_FUNCS[(typ, json_key)]
            except KeyError:
                pass
            result = compare_json_key(pairs, json_key, func)
            get_vals = lambda ix: tuple(
                filter(
                    lambda x: x is not None,
                    map(
                        lambda vid_tup: vid_tup[ix][json_key]
                        if json_key in vid_tup[ix]
                        else None,
                        pairs,
                    ),
                )
            )
            vid1_vals = get_vals(0)
            vid2_vals = get_vals(1)
            results.append((result, typ, json_key, vid1_vals, vid2_vals))
        results = sorted(results, key=lambda tup: (-tup[0], tup[1]))
        for result, typ, json_key, vid1_vals, vid2_vals in results:
            color = TermColor.Green if result else TermColor.Red
            print_d(
                color_text(json_key, color),
                shorten_string(vid1_vals, PRINT_WIDTH),
                shorten_string(vid2_vals, PRINT_WIDTH),
                verbosity_limit=1,
            )
        return results

    general_pair_tests = get_pair_tests(general_pairs)
    video_pair_tests = get_pair_tests(video_pairs)
    audio_pair_tests = get_pair_tests(audio_pairs)

    print_d("\n===GENERAL===", verbosity_limit=1)
    gen_results = all_tests_and_results(
        general_pair_tests, general_pairs, StreamType.General
    )
    print_d("\n===VIDEO===", verbosity_limit=1)
    vid_results = all_tests_and_results(video_pair_tests, video_pairs, StreamType.Video)
    print_d("\n===AUDIO===", verbosity_limit=1)
    aud_results = all_tests_and_results(audio_pair_tests, audio_pairs, StreamType.Audio)

    # tests_and_results = gen_results + vid_results + aud_results
    # print(tests_and_results)

    tests = STREAM_PROP_TESTS
    if any(
        map(lambda json: json["Format"] == "Opus", mediainfo(fname2, StreamType.Audio))
    ):
        tests = list(
            filter(
                lambda tup: not (
                    tup[0] == StreamType.Audio
                    and tup[1]
                    in ["ChannelLayout", "ChannelPositions", "Channels", "FrameRate"]
                ),
                tests,
            )
        )

    results = list(
        map(
            lambda typ_jsonkey: compare_json_key(
                get_pairs(fname1, fname2, typ_jsonkey[0]),
                typ_jsonkey[1],  # jsonkey
                STREAM_PROP_COMPARE_FUNCS[typ_jsonkey]  # compare function
                # get_pairs(fname1, fname2, typ_jsonkey[0]), *typ_jsonkey[1:]
            ),
            tests,
        )
    )
    orig_results = [(results[ix], *tests[ix]) for ix in range(len(results))]
    print(orig_results)

    return [(results[ix], *tests[ix]) for ix in range(len(results))]


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
    result_tups: list[tuple[StreamType, str]] = list(
        map(lambda tup: tup[1:], filter(lambda x: not x[0], results))
    )
    pretty_results = "\n".join(
        map(
            lambda tup: str(tup[0]) + ": " + color_text(tup[1], TermColor.Red),
            result_tups,
        )
    )
    return pretty_results


def get_converted_name(path: p.PosixPath) -> p.PosixPath:
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
    codecs = list(map(lambda obj: obj["Format"], vid_jsons))
    return any(map(lambda codec: codec in SKIP_CODECS, codecs))


def is_mp4_x265(path: p.PosixPath) -> bool:
    """
    Whether the given file is already HEVC in an MPEG-4 container.
    """
    gen_json = mediainfo(path, StreamType.General)[0]
    container = None
    try:
        container = gen_json["Format"]
    except KeyError as e:
        raise (ValueError(f"File {path} doesn't look like a video file"))
    if container != "MPEG-4":
        return False
    vid_jsons = mediainfo(path, StreamType.Video)
    codecs = list(map(lambda obj: obj["Format"], vid_jsons))
    codec_ids = list(map(lambda obj: obj["CodecID"], vid_jsons))

    return all(map(lambda codec: codec == "HEVC", codecs)) and all(
        map(lambda codec_id: codec_id == "hvc1" or codec_id == "hev1", codec_ids)
    )


def validate_streams_to_convert(
    path: p.Path, typ: StreamType, num_packets=200
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
        path,
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
    path: p.PosixPath, typ: StreamType, validate=False
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

      .. code-block::

        {
          "@type": "Text",
          "@typeorder": "2",
          "CodecID": "S_HDMV/PGS",
        }

      .. code-block:: bash

        $> ffmpeg ... -map 0:s:1 -c:s:1 dvdsub ...

      Example 2:

      .. code-block::

        {
          "@type": "Text",
          "@typeorder": "5",
          "CodecID": "S_TEXT/UTF8"
        }

      .. code-block:: bash

        $> ffmpeg ... -map 0:s:4 -c:s:4 mov_text ...

    """
    num_frames = 200
    codec_ids = list(map(lambda typ_json: typ_json["CodecID"], mediainfo(path, typ)))

    if validate:
        all_stream_ixs, invalid_ixs = validate_streams_to_convert(
            path, typ, num_packets=num_frames
        )
    else:
        invalid_ixs = []
        all_stream_ixs = set(
            map(lambda sub_json: int(sub_json["StreamOrder"]), mediainfo(path, typ))
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
        map(lambda sub_json: int(sub_json["StreamOrder"]), mediainfo(path, typ))
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


@cache()
def ffmpeg_cmd(path: p.Path) -> list[str]:
    video_tracks = mediainfo(path, StreamType.Video)
    heights = set(map(lambda track: track["Height"], video_tracks))
    widths = set(map(lambda track: track["Width"], video_tracks))

    # assert len(heights) == 1 and len(widths) == 1

    # TODO: These may not be the same index
    height = max(heights)
    width = max(widths)

    audio_codec_ids = map(
        lambda track: track["CodecID"], mediainfo(path, StreamType.Audio)
    )

    # Without a duration field, validation will fail
    subtitles_have_duration = True
    subtitle_conversions = []
    try:
        list(map(lambda track: track["Duration"], mediainfo(path, StreamType.Text)))
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
    limit=None,
    convert_and_replace=True,
    keep_failures=False,
    just_list=False,
    dry_run=False,
) -> float:
    total_size_saved = 0
    limit = len(vidlist) if (limit is None) else min(len(vidlist), limit)
    limit_digits = len(f"{limit}")

    num_processed = 0
    for cur_path in vidlist:
        # Because the size of the vidlist is iffy
        if num_processed >= limit:
            return total_size_saved

        video_streams = mediainfo(cur_path, StreamType.Video)
        video_formats = list(map(lambda stream: stream["Format"], video_streams))
        if is_mp4_x265(cur_path):
            continue
        if just_list:
            print(os.path.getsize(cur_path), cur_path)
            continue

        new_path = get_converted_name(cur_path)
        audio_streams = mediainfo(cur_path, StreamType.Audio)
        text_streams = mediainfo(cur_path, StreamType.Text)
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
            print_to_width(color_text(f"AV1/VP9 file!", TermColor.Green))
            print()
            continue

        if os.path.exists(new_path):
            print_to_width(f"Found existing conversion, verify then skip:")
            verified = verify_conversion(cur_path, new_path)
            verif_color = (
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


def human_readable(num, suffix="B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def find_compression_ratio(f1, f2):
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
        if val == None:
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
    glob_folder = lambda path: reduce(
        lambda x, y: x + y,
        map(lambda ext: list(path.glob(f"**/*{ext}")), VIDEO_FILE_EXTS),
    )
    globbed = list(map(glob_folder, folders))
    accumulated_globs = []
    if globbed:
        accumulated_globs = reduce(lambda x, y: x + y, globbed)
    vid_list = list(files) + accumulated_globs
    assert len(files) + len(folders) == len(CLI_STATE.FILES)

    # Start the clock
    orig_time = datetime.datetime.now()

    files_and_sizes = list(map(lambda file: (file, os.path.getsize(file)), vid_list))
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

    def replace_space(string):
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


def test_print_d_colors():
    global DEBUG_VERBOSITY
    DEBUG_VERBOSITY = 4
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


def test_print_d_mostly_plain():
    global DEBUG_VERBOSITY
    DEBUG_VERBOSITY = 4
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
