#!/bin/python
import _io

import argparse as ap
import humanize
import json
import math
import os
import pathlib as p
import re
import shutil
import sys
import subprocess as s
import datetime

from enum import Enum, unique
from textwrap import TextWrapper
from functools import reduce

import ffpb

os.nice(10)

# NVENC_SETTINGS = ["-tune", "hq", "-rc", "vbr", "-multipass", "qres", "-b_ref_mode", "each", "-rc-lookahead", 32, "-strict_gop", "1", "-spatial_aq", "1", "temporal_aq", "1"]
# AV1_ENCODER = "libsvtav1"

# Extensions of convertable files
VIDEO_FILE_EXTS = set([".mkv", ".mp4"])

COLLISION_SUFFIX = "converted"
PRINT_WIDTH = 68
DEBUG_VERBOSITY = None  # Set with -v switches, don't try to change it here
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
    # Audio
    "A_EAC3": "copy",
    "A_DTS": "copy",
    "A_AC3": "copy",
    "ac-3": "copy",
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

TOLERANCE = 6
CROSS = "✖"
CHECK = "✔"
GAP = "    "

GEN_CACHE = dict()



@unique
class StreamType(Enum):
    General = "General"
    Video = "Video"
    Audio = "Audio"
    Text = "Text"
    Menu = "Menu"

    def max_streams(self):
        if self == StreamType.General:
            return 1
        else:
            return None

    def ffprobe_ident(self):
        if self == StreamType.Text:
            return "s"
        return self.name[0].lower()


class TermColor(Enum):
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
            try:
                return cache_dict[func_name][arg_tuple]
            except KeyError:
                pass
            if DEBUG:
                print("Caching result for", func_name)
            result = func(*args, **kwargs)
            try:
                cache_dict[func_name][arg_tuple] = result
            except KeyError:
                cache_dict[func_name] = dict()
                cache_dict[func_name][arg_tuple] = result

            return result

        return wrapper

    return inner


@cache()
def mediainfo(path: p.Path, typ: StreamType = None) -> list[dict[StreamProperty, str]]:
    if type(path) != p.PosixPath:
        raise (TypeError("mediainfo was passed an object that isn't a pathlib.Path"))

    proc_out = s.run(["mediainfo", "--Output=JSON", path], capture_output=True)
    mediainfo_out = proc_out.stdout
    mediainfo_err = proc_out.stderr
    if mediainfo_err:
        raise (ValueError(mediainfo_err))
    file_json = json.loads(mediainfo_out)["media"]["track"]
    if typ == None:
        return [file_json]

    typ_json = list(filter(lambda obj: obj["@type"] == typ.name, file_json))
    if typ == StreamType.General:
        if len(typ_json) != 1:
            raise RuntimeError(f"Number of '{typ}' objects in '{path}' is not one")
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
    :param TermColor color: Color the text using the given terminal color.
    :param bool inv: If true, flip the background and foreground colors.
    :param bool bold: If true, bold the text.
    :param _io.TextIOWrapper outstream: The file stream to write to
      (stdout by default).
    :returns: None
    """
    if verbosity_limit > DEBUG_VERBOSITY:
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
        last_escape_in_line = escapes[-1]
        if escapes:
            inner_color = None
            try:
                inner_color = line.index(last_escape_in_line) > line.index("[0m")
            except ValueError:
                inner_color = False
        next_escape = last_escape_in_line if inner_color else escape_code
        new_result.append(line + return_code)

    outstream.write("\n".join(new_result))
    outstream.write("\n")


def color_text(string: str, color: TermColor, inv=False, bold=False) -> str:
    """
    Changes a string's color.

    :param str string: The input string
    :param TermColor color: The (ascii terminal) color
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


def verify_conversion(fname1: p.Path, fname2: p.Path) -> bool:
    def compare_json_key(pairs, key):
        return all(map(lambda f1_f2: f1_f2[0][key] == f1_f2[1][key], pairs))

    def compare_json_key_diffs(pairs, key):
        return all(
            map(
                lambda f1_f2: math.fabs(float(f1_f2[0][key]) - float(f1_f2[1][key]))
                < TOLERANCE,
                pairs,
            )
        )

    f1_general = mediainfo(fname1, StreamType.General)[0]
    f2_general = mediainfo(fname2, StreamType.General)[0]
    general_pairs = [(f1_general, f2_general)]

    f1_videos = mediainfo(fname1, StreamType.Video)
    f2_videos = mediainfo(fname2, StreamType.Video)
    if len(f1_videos) != len(f2_videos):
        return False
    video_pairs = [(f1_videos[ix], f2_videos[ix]) for ix in range(len(f1_videos))]

    f1_audios = mediainfo(fname1, StreamType.Audio)
    f2_audios = mediainfo(fname2, StreamType.Audio)
    if len(f1_audios) != len(f2_audios):
        return False
    audio_pairs = [(f1_audios[ix], f2_audios[ix]) for ix in range(len(f1_audios))]

    return (
        compare_json_key_diffs(general_pairs, "Duration")
        and compare_json_key(general_pairs, "VideoCount")
        and compare_json_key(general_pairs, "AudioCount")
        and (len(f1_videos) == len(f2_videos))
        and compare_json_key_diffs(video_pairs, "Duration")
        and compare_json_key_diffs(video_pairs, "DisplayAspectRatio")
        and compare_json_key(video_pairs, "Height")
        and compare_json_key(video_pairs, "Width")
        and compare_json_key(video_pairs, "ColorSpace")
        and (len(f1_audios) == len(f2_audios))
        and (compare_json_key_diffs(audio_pairs, "Duration"))
        and compare_json_key(audio_pairs, "ChannelPositions")
        and compare_json_key(audio_pairs, "ChannelLayout")
        and compare_json_key(audio_pairs, "Format")
    )


def change_ext(path: p.PosixPath, new_ext: str) -> str:
    return path.with_name(path.stem + "." + new_ext)


def get_converted_name(path: p.PosixPath) -> str:
    ext = path.suffix
    if ext != ".mp4":
        return change_ext(path, "mp4")
    else:
        return path.with_name(path.stem + "-converted" + ext)


def is_mp4_x265(path: p.PosixPath):
    gen_json = mediainfo(path, StreamType.General)[0]
    container = gen_json["Format"]
    if container != "MPEG-4":
        return False
    vid_jsons = mediainfo(path, StreamType.Video)
    codecs = list(map(lambda obj: obj["Format"], vid_jsons))
    if any(map(lambda codec: codec != "HEVC", codecs)):
        return False
    codec_ids = list(map(lambda obj: obj["CodecID"], vid_jsons))
    if any(map(lambda codec_id: codec_id != "hvc1" and codec_id != "hev1", codec_ids)):
        return False
    return True


def validate_conversions(path: p.Path, typ: StreamType, num_packets=200):
    ## This will not grab tracks that aren't in the first `num_packets`
    ## packets
    ffprobe_out = s.run(
        [
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
        ],
        capture_output=True,
    )
    ffprobe_stderr = ffprobe_out.stderr
    if ffprobe_stderr:
        raise (RuntimeError(f"ffprobe failure:\n{GAP}{ffprobe_stderr}"))
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


def get_stream_ix_offset(path: p.Path, typ: StreamType):
    counts = {
        "Video": int(mediainfo(path, StreamType.General)[0]["VideoCount"]),
        "Audio": int(mediainfo(path, StreamType.General)[0]["AudioCount"]),
    }

    offset = 0
    for typ_key in counts.keys():
        if typ.name == typ_key:
            break
        else:
            offset += counts[typ_key]

    return offset


## Given a file, generate the ffmpeg string to convert all subtitles
#  appropriately.
#
# @param path The input file path
#
# ffmpeg cannot convert subtitles between images and text, so the
# only conversions allowed are text->text and bitmap->bitmap.
# The examples below explain what format ffmpeg expects and how this
# function generates its result.
#
# How ffmpeg must convert subtitles
# ---------------------------------
# Get all objects with @type text, pull their @typeorder and
# CodecID. Subtract 1 from @typeorder. Then, if CodecID is
# S_TEXT/UTF8 use mov_text, otherwise dvdsub:
#
#   =========================================
#   Example
#     {
#       "@type": "Text",
#       "@typeorder": "2",
#       "CodecID": "S_HDMV/PGS",
#     }
#
#     ffmpeg ... -map 0:s:1 -c:s:1 dvdsub ...
#   =========================================
#   Example
#     {
#       "@type": "Text",
#       "@typeorder": "5",
#       "CodecID": "S_TEXT/UTF8"
#     }
#
#     ffmpeg ... -map 0:s:4 -c:s:4 mov_text ...
#
# @returns A string to be passed to ffmpeg as arguments
#
def generate_conversions(path: p.PosixPath, typ: StreamType, validate=False) -> str:
    num_frames = 200
    codec_ids = list(map(lambda typ_json: typ_json["CodecID"], mediainfo(path, typ)))

    offset = get_stream_ix_offset(path, typ)
    if validate:
        all_stream_ixs, invalid_ixs = validate_conversions(path, typ, num_packets=num_frames)
    else:
        invalid_ixs = []
        all_stream_ixs = set([i + offset for i, _ in enumerate(codec_ids)])

    # If 200 packets is too small, keep tryin'
    more_frames = False
    while len(all_stream_ixs) != len(mediainfo(path, typ)):
        num_frames *= 2
        if not more_frames:
            print(f"{GAP}{GAP}Calculated stream indexes ({len(all_stream_ixs)}) for type '{typ}'")
            print(f"{GAP}{GAP}do not match the mediainfo output ({len(mediainfo(path, typ))})")
        if not validate:
            raise AssertionError(f"{GAP}validate is false, something terrible has happened")
        sys.stdout.write(f"{GAP}{GAP}Searching {num_frames} initial frames for streams -> ")
        all_stream_ixs, invalid_ixs = validate_conversions(path, typ, num_packets=num_frames)
        print(len(all_stream_ixs))
        more_frames = True

    all_stream_ixs_json = set(
        map(lambda sub_json: int(sub_json["StreamOrder"]), mediainfo(path, typ))
    )
    if all_stream_ixs != all_stream_ixs_json:
        raise (
            ValueError(
                f"The calculated '{typ}' indices found do not match those given by mediainfo"
            )
        )

    valid_stream_ixs = all_stream_ixs.difference(invalid_ixs)

    num_codecs_so_far = 0
    encoding_args = []
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

    assert len(heights) == 1 and len(widths) == 1

    height = heights.pop()
    width = widths.pop()

    audio_codec_ids = map(
        lambda track: track["CodecID"], mediainfo(path, StreamType.Audio)
    )

    return (
        [
            "ffmpeg",
            "-canvas_size",
            f"{height}x{width}",
            "-i",
            f"{path}",
            "-strict",
            "-2",
        ]
        + generate_conversions(path, StreamType.Video)
        + generate_conversions(path, StreamType.Audio)
        + generate_conversions(path, StreamType.Text, validate=True)
        + [str(get_converted_name(path))]
    )


def process_vidlist(
    vidlist: [p.PosixPath],
    limit=None,
    convert_and_replace=True,
    remove_bad_conversions=True,
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
            print()
            continue
        ffmpeg_pretty_proc_str = pprint_ffmpeg(cur_path)
        ffmpeg_proc_str = ffmpeg_cmd(cur_path)

        pretty_proc_str = f"{GAP}" + "\n".join(ffmpeg_pretty_proc_str)
        print(pretty_proc_str)
        print()
        ffpb.main(argv=ffmpeg_proc_str[1:], stream=sys.stderr)

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
                os.remove(new_path)
                print()
            else:
                print_to_width(f"Keeping {color_text(new_path, TermColor.Red)}")
                print()
                num_processed += 1
            continue

        num_processed += 1
        print()
    # Because the size of the vidlist is accurate
    return total_size_saved


def human_readable(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def find_compression_ratio(f1, f2):
    f1_size = os.path.getsize(f1)
    f2_size = os.path.getsize(f2)
    return 100 * f2_size / f1_size


def main():
    vid_list = None
    replace = True
    try:
        vid_list = [p.Path(sys.argv[1])]
        replace = False
    except IndexError:
        vid_list = list(
            filter(
                lambda path: path.is_file(),
                list(p.Path(".").glob("**/*mkv")) + list(p.Path(".").glob("**/*mp4")),
            )
        )

    files_and_sizes = list(map(lambda file: (file, os.path.getsize(file)), vid_list))

    files_and_sizes = sorted(files_and_sizes, key=lambda video_size: -video_size[1])
    videos = list(map(lambda video_size: video_size[0], files_and_sizes))
    total_size_saved = process_vidlist(
        videos, limit=NUM_TO_PROCESS, convert_and_replace=replace
    )


if __name__ == "__main__":
    main()


def test_print_d():
    print_d(
        "hello "
        + color_text(CHECK, TermColor.Green)
        + " something a bit longer "
        + color_text(
            "what is something "
            + color_text("about", TermColor.Yellow)
            + " the watchers",
            TermColor.Blue,
        )
        + " bud"
        + " in this cruel world of ours, sometimes it is necessary",
        bold=True,
        inv=True,
        color=TermColor.Red,
    )
