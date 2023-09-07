#!/bin/python
import json
import math
import os
import pathlib as p
import re
import shutil
import sys
import textwrap
import time
import threading
import subprocess as s

from enum import Enum, auto
from textwrap import TextWrapper

import ffpb


CODEC_ID_MAP = {
    "S_TEXT/UTF8": "mov_text",
    "tx3g": "mov_text",
    "subtitle": "dvdsub",
    "A_DTS": "ac3",
    "A_AC3": "copy",
    "AAC": "copy",
    "mp4a-40-2": "copy",
}


DEBUG = False
DEBUG_VERBOSITY = 1
D_STRING = "--> "

COLLISION_SUFFIX = "-compress"
TOLERANCE = 6
CROSS = "✖"
CHECK = "✔"
GAP = "    "

GEN_CACHE = dict()


class StreamType(Enum):
    General = auto()
    Video = auto()
    Audio = auto()
    Text = auto()
    Menu = auto()

    def max_streams(self):
        if self == StreamType.General:
            return 1
        else:
            return None

    def ffprobe_ident(self):
        if self == StreamType.Text:
            return "s"
        return self.name[0].lower()


## A wrapper that causes a function's results to be memoized.
#
# Results are stored to a default dictionary, but you can supply your
# own cache dictionary if it's necessary to keep separate caches.
#
def cache(cache_dict=GEN_CACHE):
    def inner(func):
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            arg_tuple = args
            kwarg_pairs = tuple([(key, kwargs[key]) for key in kwargs])
            try:
                return cache_dict[func_name][arg_tuple + kwarg_pairs]
            except KeyError:
                pass
            if DEBUG_VERBOSITY >= 2:
                dict_args = tuple([f"{key}{kwargs[key]}" for key in kwargs])
                print(
                    f"{GAP}{GAP}{GAP}{D_STRING}Caching result for {func_name}{args + dict_args}"
                )
            result = func(*args, **kwargs)
            try:
                cache_dict[func_name][arg_tuple + kwarg_pairs] = result
            except KeyError:
                cache_dict[func_name] = dict()
                cache_dict[func_name][arg_tuple + kwarg_pairs] = result

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



def color_green(str):
    return f"[0;32m{str}[0m"


def color_red(str):
    return f"[0;31m{str}[0m"


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
    lst[4] = '"' + lst[4] + '"'
    lst[-1] = '"' + lst[-1] + '"'
    width = shutil.get_terminal_size().columns

    def write_to_width(lst, initial_indent="", subsequent_indent=GAP):
        wrapper = TextWrapper(
            initial_indent=initial_indent,
            break_long_words=False,
            width=72,
            subsequent_indent=subsequent_indent,
        )
        cmd = []
        for line in " ".join(map(str, lst)).splitlines(True):
            lst = map(lambda s: s + " \\", wrapper.wrap(line))
            cmd += lst
        return cmd

    return (
        write_to_width(lst[0:4])
        + [f"{GAP}{GAP}{lst[4]} \\"]
        + write_to_width(lst[5:-1], GAP + GAP, GAP + GAP)
        + [f"{GAP}{GAP}{lst[-1]}"]
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


def process_vidlist(vidlist: [p.PosixPath], limit=None) -> bool:
    if limit is None:
        limit = len(vidlist)
    limit_digits = len(f"{limit}")

    num_processed = 0
    for cur_path in vidlist:
        if num_processed > limit:
            return True
        video_streams = mediainfo(cur_path, StreamType.Video)
        video_formats = list(map(lambda stream: stream["Format"], video_streams))
        if is_mp4_x265(cur_path):
            continue

        new_path = get_converted_name(cur_path)
        audio_streams = mediainfo(cur_path, StreamType.Audio)
        text_streams = mediainfo(cur_path, StreamType.Text)
        format_string = "({:" + f"{limit_digits}d" + "}/{:d}) Processing {}"

        print(format_string.format(num_processed, limit, cur_path.name))
        print(f"{GAP}Dir: {cur_path.parent}")
        print(f"{GAP}New file: {new_path}")
        print()
        ffmpeg_pretty_proc_str = pprint_ffmpeg(cur_path)
        ffmpeg_proc_str = ffmpeg_cmd(cur_path)

        pretty_proc_str = f"{GAP}" + "\n".join(ffmpeg_pretty_proc_str)
        print(pretty_proc_str)
        print()
        ffpb.main(argv=ffmpeg_proc_str[1:], stream=sys.stderr)

        if verify_conversion(cur_path, new_path):
            compression = find_compression_ratio(cur_path, new_path)
            print(f"{GAP}{color_green(CHECK)} Metadata matches")
            print(f"{GAP}  Compression: {compression}%")
            print("os.replace(new_path, cur_path)")
        else:
            print(f"{GAP}{color_red(CROSS)} Metadata mismatch")
            os.remove(new_path)
        print()
        num_processed += 1


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
    try:
        vid_list = [p.Path(sys.argv[1])]
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
    process_vidlist(videos, limit=5)


if __name__ == "__main__":
    main()
