#!/bin/bash

export GAP="    "
export DEBUG=0 # 0 is true
export DRY_RUN=1
export TRASH_BIN=1
export LIMIT_TO_NUM=3
export PID="$$"
export PID_SUFFIX="_$PID"

export LOG=conversions${PID_SUFFIX}.log
export FFMPEG_LOG=ffmpeg${PID_SUFFIX}.log
export ACTIVE=active${PID_SUFFIX}.log # File storing the filename currently processing
export FIND_FUNC="find . -type f -regex '.*\(mkv\|mp4\)' -print0 | du -Sh --files0-from - | sort -hr | cut -f2"
export BAR_SIZE=40
export BIN_LOCATION="${HOME}/.trash"

# Suffix while converting an mp4 video file, to avoid name collisions.
export COLLISION_SUFFIX="-compress"

if [ ${TRASH_BIN} -eq 0 ]; then
    if [ -e "${BIN_LOCATION}" ] && [ ! -d "${BIN_LOCATION}" ]; then
        echo "Trash bin exists, but is not a directory." >&2
        echo "Exiting." >&2
        exit
    elif [ ! -e "${BIN_LOCATION}" ]; then
        mkdir "${BIN_LOCATION}"
    fi
fi

export TOLERANCE=6

export CROSS="✖"
export CHECK="✔"

export AV_LOG_FORCE_COLOR=1

# TODO: consolidate FFMPEG_CMD and PV_CMD
# export FFMPEG_CMD="ffmpeg -i pipe: -crf 26 -preset slow -strict 2 -map 0 -c:a copy -tag:v:0 hvc1"
export PV_FMT="${GAP}%t %p %r %e"

color_good() {
    local str="${*}"
    echo -n "[0;32m${str}[0m"
}

color_bad() {
    local str="${*}"
    echo -n "[0;31m${str}[0m"
}

## Delete a file.
# Deletes the given file, unless DRY_RUN or DEBUG is set to true.
# @param ${1} The path of the file to delete (rel. and/or abs.)
delete_file() {
    local fname="${1}"
    [ ! -e "${fname}" ] && return 0
    [ ${DEBUG} -eq 0 ] && echo "${GAP}rm \"${fname}\"" && return 0
    [ ${DRY_RUN} -eq 0 ] && return 0
    if [ ${TRASH_BIN} -eq 0 ]; then
        mv "${fname}" "${BIN_LOCATION}"
        return $?
    fi
    rm "${fname}"
}

## Move a file
# Move the given file, unless DRY_RUN or DEBUG is set to true
# @param ${1} The path of the file to move
# @param ${2} The path of the new location
move_file() {
    local fname="${1}"
    local dest="${2}"
    [ ! -e "${fname}" ] && return 1
    [ ${DEBUG} -eq 0 ] && echo "${GAP}mv \"${fname}\" \"${dest}\"" && return 0
    [ ${DRY_RUN} -eq 0 ] && return 0
    mv "${fname}" "${dest}"
}

## Output text to a file.
# Outputs the given text to a file, unless DRY_RUN is set to true.
# @param ${1} The path of the file to output to.
# @param ${2} The text to output.
output_to_file() {
    local out_fname="${1}"
    local out_text="${2}"
    [ ${DRY_RUN} -ne 0 ] && echo -en "${out_text}" >>"${out_fname}"
}

## Change the extension of a given *filename*.
# Given a filename, this swaps the extension for the one given. This
# does NOT touch any files on the filesystem; it only transforms
# strings.
# @param ${1} The original filename
# @param ${2} The new extension
change_extension() {
    local fname="${1}"
    local new_ext="${2}"
    local old_ext
    old_ext="$(get_extension "${fname}")"
    echo "${fname//"${old_ext}"/"${new_ext}"}"
}

## Add a string to the name of a file, before the extension
# @param ${1} The name of the file
# @param ${2} The string to append
append_name() {
    local fname="${1}"
    local app="${2}"
    local ext
    ext="$(get_extension "${fname}")"
    echo "${fname//."${ext}"/${app}.${ext}}"
}

## Find the extension of a file.
# @param ${1} The filename
get_extension() {
    local fname="${1}"
    echo "${fname//*./}"
}

## Checks to see if a given file is a video file.
# Currently this function just uses the mimetype to see if the file is
# a video file.
# @param ${1} The file to test.
is_video_file() {
    local fname="${1}"
    local mime_type
    mime_type="$(dirname "$(xdg-mime query filetype "${fname}")")"
    [ "${mime_type}" = "video" ]
}

## Draw a (ncurses-like) progress bar in the terminal
# The progress bar will be of length ${BAR_SIZE}. Note that the bar does
# not end in a newline character.
# @param ${1} Current numeric progress (e.g. number of files scanned)
# @param ${2} Total items in progress (e.g. number of files to scan)
draw_progress_bar() {
    cur="${1}"
    tot="${2}"
    echo -n "["
    ratio=$(echo "print(${cur} / ${tot})" | python)
    percent=$(echo "print(f'{100 * ${ratio}:.2f}')" | python)
    num_prog_dots=$(echo \
        "import math; print(math.ceil((${BAR_SIZE} - 2) * ${ratio}))" \
        | python)
    num_prog_space=$(echo \
        "import math; print(math.floor(${BAR_SIZE} - 2 - ${num_prog_dots}))" \
        | python)
    for ((i = 0; i < num_prog_dots; i++)); do
        echo -n "#"
    done
    for ((i = 0; i < num_prog_space; i++)); do
        echo -n "."
    done
    echo -n "] ${percent}% (${cur}/${tot})"
    echo -en "\r"
}

is_converted() {
    local file1="${1}"
    local f1Video
    local ext
    f1Video="$(mediainfo --Output=JSON "$file1" \
        | jq '.media.track[] | select(.["@type"] == "Video")')"
    if [ "$(echo "$f1Video" | jq -r '.Format' | head -n1)" != "HEVC" ]; then
        return 1
    elif [ "$(echo "$f1Video" | jq -r '.CodecID' | head -n1)" != "hev1" ] \
        && [ "$(echo "$f1Video" | jq -r '.CodecID' | head -n1)" != "hvc1" ]; then
        return 1
    fi
    ext="$(get_extension "${file1}")"
    if [ "${ext}" != "mp4" ]; then
        return 1
    fi
    return 0
}

## Given a video file, checks the parent directory to see
# if there is a compressed video file to match
#
# @param ${1} The name of the file to check. If this file is not a
# video file, then this function returns false
contains_converted() {
    local fname="${1}"
    local norm_fname
    local other_fname
    local norm_other_fname
    local dir
    if ! is_video_file "${fname}"; then
        return 1
    fi
    norm_fname="$(change_extension "${fname}" "")"
    norm_fname="$(basename "${norm_fname//${COLLISION_SUFFIX}/}")"
    dir="$(dirname "${fname}")"
    while read -r other_fname; do
        norm_other_fname="$(change_extension "${other_fname}" "")"
        norm_other_fname="$(basename "${norm_other_fname//${COLLISION_SUFFIX}/}")"
        if [ "${fname}" = "${other_fname}" ]; then
            continue
        elif [ "${norm_fname}" = "${norm_other_fname}" ]; then
            if ! is_video_file "${other_fname}"; then
                continue
            elif ! is_converted "${other_fname}"; then
                continue
            elif verify_conversion "${fname}" "${other_fname}"; then
                echo "${GAP}$(color_good ${CHECK}) Metadata matches"
                echo "${GAP}  Compression: $(find_compression_ratio "${fname}" "${other_fname}")"
                return 0
            fi
        fi
    done < <(find "${dir}" -maxdepth 1 -type f)
    return 1
}

find_compression_ratio() {
    local file1="${1}"
    local file2="${2}"

    f1size="$(du -b "${file1}" | cut -f1)"
    f2size="$(du -b "${file2}" | cut -f1)"
    percent="$(echo "print(\"{:.1%}\".format(${f2size}/${f1size}))" | python)"
    echo "${percent}"
}

## Verify that two media files contain the same content through an ffmpeg conversion.
# Verification is done through several checks:
#   - General
#       * The number of streams match.
#       * The duration of corresponding streams match (e.g. Audio <-> Audio)
#       * Framerate, frame count matches
#   - Video specific
#       * Width & Height match
#       * Colorspace matches
#       * ScanType matches
#       * New codec is HEVC, old codec is not
#       * Aspect ratio matches
#   - Audio specific
#       * Format
#       * BitRate (and BR mode)
#       * Num, Positions, and Layout of Channels
#
# @param ${1} The original file
# @param ${2} The converted file
verify_conversion() {
    local file1="$1"
    local file2="$2"
    local f1Full
    local f2Full
    local f1General
    local f2General
    local f1Video
    local f2Video
    local f1Audio
    local f2Audio
    local duration1
    local duration2
    local durationDiff
    f1Full="$(mediainfo --Output=JSON "${file1}")"
    f2Full="$(mediainfo --Output=JSON "${file2}")"
    f1General="$(echo "${f1Full}" \
        | jq '.media.track[] | select(.["@type"] == "General")')"
    f2General="$(echo "$f2Full" \
        | jq '.media.track[] | select(.["@type"] == "General")')"
    duration1="$(echo "$f1General" | jq -r '.Duration')"
    duration2="$(echo "$f2General" | jq -r '.Duration')"
    [ "$duration1" = null ] && return 1 # if the conversion failed,
    [ "$duration2" = null ] && return 1 # duration will be empty
    durationDiff=$(echo \
        "import math; print(math.fabs(${duration1} - ${duration2}) < $TOLERANCE)" \
        | python)
    [ "$(echo "$f1General" \
        | jq -r '.VideoCount')" -ne "$(echo "$f2General" \
            | jq -r '.VideoCount')" ] && return 1
    [ "$(echo "$f1General" \
        | jq -r '.AudioCount')" -ne "$(echo "$f2General" \
            | jq -r '.AudioCount')" ] && return 1
    [ "$durationDiff" != "True" ] && return 1

    f1Video="$(echo "${f1Full}" \
        | jq '.media.track[] | select(.["@type"] == "Video")')"
    f2Video="$(echo "${f2Full}" \
        | jq '.media.track[] | select(.["@type"] == "Video")')"
    duration1="$(echo "$f1Video" | jq -r '.Duration' | head -n1)"
    duration2="$(echo "$f2Video" | jq -r '.Duration' | head -n1)"
    durationDiff=$(echo \
        "import math; print(math.fabs(${duration1} - ${duration2}) < $TOLERANCE)" \
        | python)
    aspect1="$(echo "$f1Video" | jq -r '.DisplayAspectRatio' | head -n1)"
    aspect2="$(echo "$f2Video" | jq -r '.DisplayAspectRatio' | head -n1)"
    aspectRatio=$(echo \
        "import math; print(math.fabs(${aspect1} - ${aspect2}) < $TOLERANCE)" \
        | python)
    [ "$(echo "$f1Video" | jq -r '.Height' | head -n1)" \
        -ne "$(echo "$f2Video" | jq -r '.Height' | head -n1)" ] \
        && return 1
    [ "$(echo "$f1Video" | jq -r '.Width' | head -n1)" \
        -ne "$(echo "$f2Video" | jq -r '.Width' | head -n1)" ] \
        && return 1
    [ "$(echo "$f1Video" | jq -r '.ColorSpace' | head -n1)" != \
        "$(echo "$f2Video" | jq -r '.ColorSpace' | head -n1)" ] \
        && return 1
    [ "$(echo "$f2Video" | jq -r '.Format' | head -n1)" != "HEVC" ] && return 1
    [ "$(echo "$f2Video" | jq -r '.CodecID' | head -n1)" != "hev1" ] \
        && [ "$(echo "$f2Video" | jq -r '.CodecID' | head -n1)" != "hvc1" ] \
        && return 1
    [ "$durationDiff" != "True" ] && return 1
    [ "$aspectRatio" != "True" ] && return 1

    f1Audio="$(echo "${f1Full}" \
        | jq '.media.track[] | select(.["@type"] == "Audio")')"
    f2Audio="$(echo "${f2Full}" \
        | jq '.media.track[] | select(.["@type"] == "Audio")')"
    duration1="$(echo "${f1Audio}" | jq -r '.Duration' | head -n1)"
    duration2="$(echo "${f2Audio}" | jq -r '.Duration' | head -n1)"
    durationDiff=$(echo \
        "import math; print(math.fabs(${duration1} - ${duration2}) < $TOLERANCE)" \
        | python)
    [ "$(echo "$f1Audio" | jq -r '.ChannelPositions' | head -n1)" != \
        "$(echo "$f2Audio" | jq -r '.ChannelPositions' | head -n1)" ] && return 1
    [ "$(echo "$f1Audio" | jq -r '.ChannelLayout' | head -n1)" != \
        "$(echo "$f2Audio" | jq -r '.ChannelLayout' | head -n1)" ] && return 1
    [ "$(echo "$f1Audio" | jq -r '.Format' | head -n1)" != \
        "$(echo "$f2Audio" | jq -r '.Format' | head -n1)" ] && return 1
    [ "$durationDiff" != "True" ] && return 1

    return 0

    # TODO: Multiple audio or video tracks?
}
