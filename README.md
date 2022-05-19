# Media Library Compression

A script to automatically convert video files to mp4 containers
holding videos encoded using x265 (HEVC). For MacOS compatibility,
this also stores metadata using hvc1 (rather than hev1).

## Installation

This is a bash script, so no installation necessary. It does depend on
having the following programs installed:
  * ffmpeg
  * jq
  * mediainfo
  * pv
  * python
  * xdg-mime (xdg-utils package in ArchLinux)

## Usage

To batch convert every video below the current working directory, run
mass_convert.sh from the directory that contains the video files. This
is a destructive operation (it will delete files after converting),
however you can use debug mode or do a dry run by changing the flags
in mass_convert_source.sh

To experiment interactively, you can source mass_convert_source.sh
from your current bash/zsh/sh session and use the functions therein.

## TODO

  * Use command line arguments to select the directory rather than
    depend on the working directory.
  * Use cli flags to change modes (e.g. debug, dry run) rather than
    editing the 'header' file.
  * Possibly create a simple config.ini file.
