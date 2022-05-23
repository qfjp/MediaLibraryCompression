#!/bin/bash

# TODO:
#  * Better handling of multiple jq objects of same .["@type"]
#    Currently this is handled by only checking the results from the
#    first object (.select.some.object | head -n1)
#  * Trap handling/cleanup

source "$(pwd)/mass_convert_source.sh"
[ -e "${FFMPEG_LOG}" ] && rm "${FFMPEG_LOG}"

tot_files="$(${FIND_FUNC} | wc -l)"
if [ $LIMIT_TO_NUM -gt 0 ]; then
    tot_files="$LIMIT_TO_NUM"
fi
num_digits_in_tot_file="$(echo \
    "import math; print(1 + math.floor(math.log10($tot_files)))" | python)"
num_processed=0
while read -r fname; do
    [ "${num_processed}" -gt 0 ] && echo
    is_mp4=1
    # Pre-Processing (increment count, draw progress bar, ...)
    if ! is_video_file "${fname}"; then
        continue
    fi
    printf "(%${num_digits_in_tot_file}d/%d) " "${num_processed}" "${tot_files}"
    echo "Processing $(basename "${fname}")"
    echo "${GAP}Dir: $(dirname "${fname}")"
    [ -e ${ACTIVE} ] && rm "${ACTIVE}"
    output_to_file "${ACTIVE}" "${fname}"

    # Processing (convert file)
    info_json="$(mediainfo --Output=JSON "${fname}")"
    vid_codec="$(echo "${info_json}" \
        | jq -r '.media.track[] | select(.["@type"] == "Video").Format' | head -n1)"
    vid_container="$(echo "${info_json}" \
        | jq -r '.media.track[] | select(.["@type"] == "General").Format' \
        | head -n1)"

    new_name="$(change_extension "${fname}" mp4)"
    sup_name="$(change_extension "${fname}" en.sup)" # fname of bitmapped subtitles
    if is_converted "${fname}"; then
        echo "${GAP}$(color_good ${CHECK}) Already compressed, skipping"
        continue
    elif contains_converted "${fname}"; then
        echo "${GAP}$(color_good ${CHECK}) Compressed exists, skipping"
    elif [ "${vid_container}" = "MPEG-4" ]; then
        new_name="$(append_name "${fname}" "-compress")"
        is_mp4=0
    fi
    ((num_processed += 1))

    echo "${GAP}New file: ${new_name}"

    is_success=1
    delete_file "${new_name}"
    if [ "${DRY_RUN}" -eq 0 ]; then
        sleep 10 | pv && is_success=0
    elif [ "${vid_codec}" = "HEVC" ]; then # If HEVC in the wrong container, copy everything
        pv -F "${PV_FMT}" -w 72 "${fname}" \
            | ffmpeg -i pipe: -strict -2 -map 0 -c copy -tag:v:0 hvc1 \
                "${new_name}" 2>>"$FFMPEG_LOG" \
            && is_success=0
    else
        pv -F "${PV_FMT}" -w 72 "${fname}" \
            | ffmpeg -i pipe: -strict -2 -map 0 -c:a copy -tag:v:0 hvc1 \
                -c:v hevc -c:s mov_text "${new_name}" 2>>"${FFMPEG_LOG}" \
            && is_success=0
    fi

    if [ "${is_success}" -ne 0 ]; then
        [ -e "${new_name}" ] && rm "${new_name}" # Remove regardless of debug mode
        [ -e "${sup_name}" ] && rm "${sup_name}" # Ditto
        echo "${GAP}$(color_bad ${CROSS}) Failed, try bitmapped subtitles"
        pv -F "${PV_FMT}" -w 72 "${fname}" \
            | ffmpeg -i pipe: -strict -2 -map 0 -c:a copy -tag:v:0 hvc1 \
                -c:v:0 hevc -c:v:1 copy -sn "${new_name}" -c:s copy \
                "${sup_name}" 2>>"${FFMPEG_LOG}" \
            && is_success=0
    fi

    output_to_file "${FFMPEG_LOG}" "==========\n==========\n==========\n\n"

    # Post-Processing (close progress bar, print info; delete?)
    if [ "${is_success}" -ne 0 ]; then
        echo "${GAP}$(color_bad ${CROSS}) ffmpeg failed"
        delete_file "${new_name}"
        [ $DEBUG -ne 0 ] && continue # only continue if debug is off
    else
        echo "${GAP}$(color_good ${CHECK}) ffmpeg succeeded"
        output_to_file "${LOG}" "${new_name}"
    fi
    if verify_conversion "${fname}" "${new_name}"; then
        echo "${GAP}$(color_good ${CHECK}) Metadata matches"
    else
        echo "${GAP}$(color_bad ${CROSS}) Metadata mismatch"
        [ $DEBUG -ne 0 ] && continue # only continue if debug is off
    fi
    delete_file "${fname}"
    # If the old file is an mp4, move the new file to its place
    [ ${is_mp4} -eq 0 ] && move_file "${new_name}" "${fname}"
    [ "${LIMIT_TO_NUM}" -gt 0 ] && [ ${num_processed} -ge ${tot_files} ] && break
    echo

done < <(${FIND_FUNC} | sort)
