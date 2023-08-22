#!/bin/python
import os
import shutil
import sys
import textwrap
import time
import threading
from textwrap import TextWrapper
import pathlib as p
import subprocess
from subprocess import PIPE, STDOUT
import json
import math
import re
from enum import Enum
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


class progress_bar_loading(threading.Thread):
    def run(self):
        global stop
        global kill
        print("Loading.... ")
        sys.stdout.flush()
        i = 0
        while stop != True:
            if (i % 4) == 0:
                sys.stdout.write("\b/")
            elif (i % 4) == 1:
                sys.stdout.write("\b-")
            elif (i % 4) == 2:
                sys.stdout.write("\b\\")
            elif (i % 4) == 3:
                sys.stdout.write("\b|")
            sys.stdout.flush()
            time.sleep(0.2)
            i += 1

        if kill == True:
            print("\b\b\b\b Abort!")
        else:
            print("\b\b\b Done!")


DEBUG = False

TRASH_BIN = False
LIMIT_TO_NUM = False
LOG_FILE = "conversions.log"
ALOG = "attempted_conversions.log"
FFMPEG_LOG = "ffmpeg.log"
# FIND_FUNC = ["find", ".", "-type", "f", "-regex", ".*\(mkv\|mp4\)", "-print0"]
BIN_LOCATION = os.environ["HOME"]
COLLISION_SUFFIX = "-compress"
TOLERANCE = 6
CROSS = "✖"
CHECK = "✔"
GAP = "    "
PV_FMT = "%t %p %r %e"
TEXT_SUBTITLE_IDS = ["tx3g", "S_TEXT/UTF8"]

GEN_CACHE = dict()


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
def mediainfo(path: p.Path):
    if type(path) != p.PosixPath:
        raise (TypeError("mediainfo was passed an object that isn't a pathlib.Path"))

    proc_out = subprocess.run(["mediainfo", "--Output=JSON", path], capture_output=True)
    mediainfo_out = proc_out.stdout
    mediainfo_err = proc_out.stderr
    if mediainfo_err:
        raise (ValueError(mediainfo_err))
    file_json = json.loads(mediainfo_out)["media"]["track"]
    return file_json


@cache()
def mediainfo_general(path: p.Path):
    file_json = mediainfo(path)
    gen_json = list(filter(lambda obj: obj["@type"] == "General", file_json))
    if len(gen_json) != 1:
        raise (Exception(f"Number of 'General' objects in {path} is not one."))
    else:
        gen_json = gen_json[0]
    return gen_json


@cache()
def mediainfo_video(path: p.Path):
    file_json = mediainfo(path)
    vid_json = list(filter(lambda obj: obj["@type"] == "Video", file_json))
    return vid_json


@cache()
def mediainfo_audio(path: p.Path):
    file_json = mediainfo(path)
    aud_json = list(filter(lambda obj: obj["@type"] == "Audio", file_json))
    return aud_json


@cache()
def mediainfo_subtitle(path: p.Path):
    file_json = mediainfo(path)
    txt_json = list(filter(lambda obj: obj["@type"] == "Text", file_json))
    return txt_json


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

    f1_general = mediainfo_general(fname1)
    f2_general = mediainfo_general(fname2)
    general_pairs = [(f1_general, f2_general)]

    f1_videos = mediainfo_video(fname1)
    f2_videos = mediainfo_video(fname2)
    if len(f1_videos) != len(f2_videos):
        return False
    video_pairs = [(f1_videos[ix], f2_videos[ix]) for ix in range(len(f1_videos))]

    f1_audios = mediainfo_audio(fname1)
    f2_audios = mediainfo_audio(fname2)
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


## Has caused problems in the past
# [ "$(echo "${f1Video}" | jq -r '.Height' | head -n1)" \
#    -ne "$(echo "${f2Video}" | jq -r '.Height' | head -n1)" ] \
#    && return 1

# [ "$(echo "${f1Video}" | jq -r '.Width' | head -n1)" \
#    -ne "$(echo "${f2Video}" | jq -r '.Width' | head -n1)" ] \
#    && return 1
# [ "$(echo "${f1Video}" | jq -r '.ColorSpace' | head -n1)" != \
#    "$(echo "${f2Video}" | jq -r '.ColorSpace' | head -n1)" ] \
#    && return 1
# [ "$(echo "${f2Video}" | jq -r '.Format' | head -n1)" != "HEVC" ] && return 1
# [ "$(echo "${f2Video}" | jq -r '.CodecID' | head -n1)" != "hev1" ] \
#    && [ "$(echo "${f2Video}" | jq -r '.CodecID' | head -n1)" != "hvc1" ] \
#    && return 1
# [ "${durationDiff}" -eq 0 ] && return 1
# [ "${aspectRatio}" -eq 0 ] && return 1

# f1Audio="$(echo "${f1_json}" \
#    | jq '.media.track[] | select(.["@type"] == "Audio")')"
# f2Audio="$(echo "${f2_json}" \
#    | jq '.media.track[] | select(.["@type"] == "Audio")')"
# duration1="$(echo "${f1Audio}" | jq -r '.Duration' | head -n1)"
# duration2="$(echo "${f2Audio}" | jq -r '.Duration' | head -n1)"
# durationDiff="$(bc << HERE
# $BC_PREFIX
# abs(${duration1} - ${duration2}) < $TOLERANCE
# HERE#
# )"

## Has caused problems in the past
##  instead, count tokens in ChannelLayout
##[ "$(echo "${f1Audio}" | jq -r '.ChannelPositions' | head -n1)" != \
##    "$(echo "${f2Audio}" | jq -r '.ChannelPositions' | head -n1)" ] && return 1
##[ "$(echo "${f1Audio}" | jq -r '.ChannelLayout' | head -n1)" != \
##    "$(echo "${f2Audio}" | jq -r '.ChannelLayout' | head -n1)" ] && return 1

##[ "$(echo "${f1Audio}" | jq -r '.Format' | head -n1)" != \
##    "$(echo "${f2Audio}" | jq -r '.Format' | head -n1)" ] && return 1
# [ "${durationDiff}" -eq 0 ] && return 1

# return 0

## TODO: Multiple audio or video tracks?


def change_ext(path: p.PosixPath, new_ext: str) -> str:
    return path.with_name(path.stem + "." + new_ext)


def get_converted_name(path: p.PosixPath) -> str:
    ext = path.suffix
    if ext != ".mp4":
        return change_ext(path, "mp4")
    else:
        return path.with_name(path.stem + "-converted" + ext)


def is_mp4_x265(path: p.PosixPath):
    gen_json = mediainfo_general(path)
    container = gen_json["Format"]
    if container != "MPEG-4":
        return False
    vid_jsons = mediainfo_video(path)
    codecs = list(map(lambda obj: obj["Format"], vid_jsons))
    if any(map(lambda codec: codec != "HEVC", codecs)):
        return False
    codec_ids = list(map(lambda obj: obj["CodecID"], vid_jsons))
    if any(map(lambda codec_id: codec_id != "hvc1" and codec_id != "hev1", codec_ids)):
        return False
    return True


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
# Valid *Stream* Indexes (called StreamOrder by mediainfo) can be
# found with:
#
#   declare -a valid_stream_indices
#   mapfile -t valid_stream_indices < <( \
#       ffprobe -loglevel error -select_streams s \
#         -show_entries packet=stream_index,duration \
#         -of csv $fname \
#     | grep -v 'N/A' \
#     | cut -d, -f2 | sort -h | uniq \
#   )
#
# @returns A string to be passed to ffmpeg as arguments
#
def generate_sub_conversions(path: p.PosixPath) -> str:
    text_json = mediainfo_subtitle(path)

    ffprobe_out = subprocess.run(
        [
            "ffprobe",
            "-loglevel",
            "error",
            "-read_intervals",
            "0:00%+#200",
            "-select_streams",
            "s",
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
    if len(all_stream_ixs) != len(mediainfo_subtitle(path)):
        raise (IndexError("Not enough packets were used to search for subtitles"))
    all_stream_ixs_json = set(
        map(lambda sub_json: int(sub_json["StreamOrder"]), mediainfo_subtitle(path))
    )
    valid_stream_ixs = all_stream_ixs.difference(invalid_ixs)
    if all_stream_ixs != all_stream_ixs_json:
        raise (
            ValueError(
                "The subtitle indices found do not match those given by mediainfo"
            )
        )

    vid_count = int(mediainfo_general(path)["VideoCount"])
    aud_count = int(mediainfo_general(path)["AudioCount"])

    type_ixs = [ix - vid_count - aud_count for ix in all_stream_ixs]
    codec_ids = list(
        map(lambda sub_json: sub_json["CodecID"], mediainfo_subtitle(path))
    )

    num_codecs_so_far = 0
    encoding_args = []
    for num_codecs_so_far, stream_ix in enumerate(valid_stream_ixs):
        type_ix = stream_ix - vid_count - aud_count
        codec_id = codec_ids[type_ix]
        encoding = None
        try:
            encoding = CODEC_ID_MAP[codec_id]
        except:
            encoding = CODEC_ID_MAP["subtitle"]
        encoding_args += [
            "-map",
            f"0:s:{type_ix}",
            f"-c:s:{num_codecs_so_far}",
            f"{encoding}",
        ]
    return encoding_args

    ## This will not grab text tracks that aren't in the first 200
    ## packets

    # num_codecs_so_far=0
    # next_valid_ix=${valid_stream_ixs[0]}
    # [ -z "$next_valid_ix" ] && return 0
    # for ((ix = 0; ix < ${#stream_ixs[@]}; ix++)); do
    #    [ -z "$next_valid_ix" ] && break
    #    [ "${stream_ixs[$ix]}" -ne "$next_valid_ix" ] && continue
    #    type_ix=$((type_ixs[ix]))
    #    codec="${codec_ids[$ix]}"
    #    echo -n " -map 0:s:$type_ix -c:s:$num_codecs_so_far "
    #    [ "${codec:0:6}" = S_TEXT -o "${codec}" = "tx3g" ] && echo -n "mov_text" || echo -n dvdsub
    #    num_codecs_so_far=$((num_codecs_so_far + 1))
    #    next_valid_ix=${valid_stream_ixs[$num_codecs_so_far]}
    # done


def pprint_ffmpeg(path: p.Path) -> str:
    lst = ffmpeg_cmd(path).copy()
    lst[4] = '"' + lst[4] + '"'
    lst[-1] = '"' + lst[-1] + '"'
    width = shutil.get_terminal_size().columns

    def write_to_width(lst, initial_indent="", subsequent_indent=GAP):
        wrapper = TextWrapper(
            initial_indent=initial_indent, break_long_words=False, width=72, subsequent_indent=subsequent_indent
        )
        cmd = []
        for line in " ".join(map(str, lst)).splitlines(True):
            lst = map(lambda s: s + " \\", wrapper.wrap(line))
            cmd += lst
        return cmd

    return (write_to_width(lst[0:4]) + [f"{GAP}{GAP}{lst[4]} \\"] + write_to_width(lst[5:-1], GAP + GAP, GAP + GAP) + [f"{GAP}{GAP}{lst[-1]}"])


@cache()
def ffmpeg_cmd(path: p.Path) -> list[str]:
    video_tracks = mediainfo_video(path)
    heights = set(map(lambda track: track["Height"], video_tracks))
    widths = set(map(lambda track: track["Width"], video_tracks))

    assert len(heights) == 1 and len(widths) == 1

    height = heights.pop()
    width = widths.pop()

    audio_codec_ids = map(lambda track: track["CodecID"], mediainfo_audio(path))
    video_codec_ids = map(lambda track: track["CodecID"], mediainfo_audio(path))

    audio_conversions = []
    for ix, codec_id in enumerate(audio_codec_ids):
        audio_conversions += ["-map", f"0:a:{ix}", f"-c:a:{ix}", CODEC_ID_MAP[codec_id]]

    return (
        [
            "ffmpeg",
            "-canvas_size",
            f"{height}x{width}",
            "-i",
            f"{path}",
            # "pipe:",
            "-strict",
            "-2",
            "-map",
            "0:v",
            "-c:v",
            "libx265",
            "-tag:v:0",
            "hvc1",
        ]
        + audio_conversions
        + generate_sub_conversions(path)
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
        video_streams = mediainfo_video(cur_path)
        video_formats = list(map(lambda stream: stream["Format"], video_streams))
        if is_mp4_x265(cur_path):
            continue

        cur_json = mediainfo(cur_path)
        new_path = get_converted_name(cur_path)
        audio_streams = mediainfo_audio(cur_path)
        text_streams = mediainfo_subtitle(cur_path)
        format_string = "({:" + f"{limit_digits}d" + "}/{:d}) Processing {}"

        print(format_string.format(num_processed, limit, cur_path.name))
        print(f"{GAP}Dir: {cur_path.parent}")
        print(f"{GAP}New file: {new_path}")
        print()
        pv_proc_str = [GAP, "pv", "-F", '"' + PV_FMT +'"', "-w", "72", '"' + str(cur_path) + '"']
        ffmpeg_pretty_proc_str = pprint_ffmpeg(cur_path)
        ffmpeg_proc_str = ffmpeg_cmd(cur_path)

        # Fix pipe
        #ffmpeg_pretty_proc_str[0] = ffmpeg_proc_str[0][:-1] + "pipe: \\"
        #ffmpeg_pretty_proc_str.pop(1)
        #ffmpeg_proc_str[4] = "pipe:"
        #ffmpeg_proc_str[4] = '"' + ffmpeg_proc_str[4] + '"'
        #ffmpeg_proc_str[-1] = '"' + ffmpeg_proc_str[-1] +'"'

        #pretty_proc_str = " ".join(pv_proc_str) + f"\\ \n{GAP}{GAP}| " + "\n".join(ffmpeg_pretty_proc_str)
        #full_proc_str = " ".join(pv_proc_str) + f" | " + " ".join(ffmpeg_proc_str) + " 2>/dev/null"
        pretty_proc_str = f"{GAP}" + "\n".join(ffmpeg_pretty_proc_str)
        #full_proc_str = " ".join(ffmpeg_proc_str) + " 2>/dev/null"
        print(pretty_proc_str)
        print()
        #print(ffmpeg_proc_str)
        ffpb.main(argv=ffmpeg_proc_str[1:], stream=sys.stderr)
        #subprocess.call(full_proc_str, shell=True)

        #def ffmpeg_call():
        #    subprocess.run(ffmpeg_cmd(cur_path))

        #kill = False
        #stop = False
        #try:
        #    ffmpeg_call()
        #    time.sleep(1)
        #    stop = True
        #except KeyboardInterrupt or EOFError:
        #    kill = True
        #    stop = True

        # verify_conversion(cur_path, new_path)
        if verify_conversion(cur_path, new_path):
            compression = find_compression_ratio(cur_path, new_path)
            print(f"{GAP}{color_green(CHECK)} Metadata matches")
            print(f"{GAP}  Compression: {human_readable(compression)}")
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
    return 100 * f1_size / f2_size


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
