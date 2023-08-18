#!/bin/bash

# TODO:
#  * Better handling of multiple jq objects of same .["@type"]
#    Currently this is handled by only checking the results from the
#    first object (.select.some.object | head -n1)
#  * Trap handling/cleanup

RUN_DIR="$(dirname "$0")"

source "${RUN_DIR}/mass_convert_source.sh"
delete_file "${FFMPEG_LOG}"

tot_files="$(eval "$FIND_FUNC" | wc -l)"
if [ $LIMIT_TO_NUM -gt 0 ] && [ $LIMIT_TO_NUM -lt "$tot_files" ]; then
    tot_files="$LIMIT_TO_NUM"
fi
num_digits_in_tot_file="$(echo \
    "import math; print(1 + math.floor(math.log10($tot_files)))" | python)"
num_processed=0
while read -r fname; do
    [ "${LIMIT_TO_NUM}" -gt 0 ] && [ ${num_processed} -ge ${tot_files} ] && break

    # Verify the file hasn't been converted
    set_mediainfo "$fname"
    container="$(mediainfo_container "$fname")"
    codec_codec_id="$(mediainfo_codec "${fname}")"
    codec="${codec_codec_id%%' '*}"
    codec_id="${codec_codec_id##*' '}"

    [[ "$container" = "MPEG-4" ]] \
        && [[ $codec = "HEVC" ]] \
        && {
        [[ "$codec_id" = "hev1" ]] \
        || [[ "$codec_id" = "hvc1" ]]
    } && continue
    num_processed=$((num_processed += 1))

    audio_codec="copy"

    [ "${num_processed}" -gt 0 ] && echo
    is_mp4=1
    # Pre-Processing (increment count, draw progress bar, ...)
    printf "(%${num_digits_in_tot_file}d/%d) " "${num_processed}" "${tot_files}"
    echo "Processing $(basename "${fname}")"
    echo "${GAP}Dir: $(dirname "${fname}")"
    delete_file "${ACTIVE}"
    output_to_file "${ACTIVE}" "${fname}"

    # Processing (convert file)
    sup_name="$(change_extension "${fname}" en.sup)" # fname of bitmapped subtitles
    new_name="$(name_to_convert_to "${fname}")"
    delete_file "${new_name}"

    if contains_converted "${fname}"; then
        echo "${GAP}$(color_good ${CHECK}) Compressed (and verified) exists, skipping"
        continue
    elif [[ "$(mediainfo_container "${fname}")" = "MPEG-4" ]]; then
        audio_codec=ac3
        is_mp4=0
    fi

    echo "${GAP}New file: ${new_name}"

    is_success=1
    codec_codec_id="$(mediainfo_codec "${fname}")"
    codec="${codec_codec_id%%' '*}"
    codec_id="${codec_codec_id##*' '}"
    if [ "${DRY_RUN}" -eq 0 ]; then
        sleep 10 | pv && is_success=0
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
        echo "${GAP}  Compression: $(find_compression_ratio "${fname}" "${new_name}")"
        echo "${GAP}  Deleting ${fname}"

        # Delete original video
        echo "delete_file \"${fname}\""

        # If the old file is an mp4, move the new file to its place
        [ ${is_mp4} -eq 0 ] && echo "move_file \"${new_name}\" \"${fname}\""
    else
        echo "${GAP}$(color_bad ${CROSS}) Metadata mismatch"
        [ $DEBUG -ne 0 ] && continue # only continue if debug is off
    fi
done < <(eval "$FIND_FUNC")
