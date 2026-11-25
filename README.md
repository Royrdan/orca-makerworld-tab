# MakerWorld Tab for OrcaSlicer

Browse [MakerWorld](https://makerworld.com) inside OrcaSlicer, the way Bambu Studio does, and open any model straight onto the Prepare page.

## Features

- **Discover:** home feeds (Trending, Featured, New, Staff Picks), the full category tree, contests and their entries, curated collections, and MakerLab models
- **Search** with sort and filters (category, licence, date)
- **Your account:** your collections, liked models and download history
- **Model details:** gallery, designer, description, stats, tags, licence, and every print profile (plates, print time, filament)
- **One-click open:** pick a print profile and it downloads into OrcaSlicer's own downloads folder and opens in your running OrcaSlicer window
- Infinite scroll, progress pop-ups and readable errors

## Requirements

- OrcaSlicer **2.5 or newer** (needs Python plugin support)
- **Linux, Flatpak build of OrcaSlicer.** Browsing works everywhere. Opening a model hands the file to OrcaSlicer through the Flatpak launcher, so on Windows, macOS or AppImage builds the download works but you open the file yourself for now.
- In OrcaSlicer **Preferences → General**, turn on the **single instance** option. Otherwise models open in a second window.

## Install

1. Download `makerworld_tab.py` from this repository (or the latest release).
2. In OrcaSlicer: **File → Plugins → Install local plugin** and pick the file.
3. Restart OrcaSlicer. A **MakerWorld** tab appears after the built-in tabs.
4. Approve the permission prompt the first time (the plugin needs to save its download and start OrcaSlicer to open it).

## Signing in

Browsing needs no account. Downloading needs your Bambu Lab account:

- **Email + password:** type both and press Log in.
- **Google / Apple sign-in accounts:** type your email, leave the password empty and press Log in. Bambu emails you a code; enter it and press Log in again.

The sign-in is saved only on your computer, in OrcaSlicer's config folder (`makerworld_tab_token.json`). **Sign out** deletes it.

## How it works

The plugin adds an OrcaSlicer "Pages" tab. The tab is a small web page; clicks are sent to the Python side of the plugin, which talks to MakerWorld's web API and sends the results back. OrcaSlicer's plugin sandbox asks before the plugin touches files or starts programs.

## Notes

- Unofficial community plugin. Not made by or affiliated with Bambu Lab, MakerWorld or the OrcaSlicer team.
- It uses the same web API as the MakerWorld website, which is not a published, supported API. MakerWorld may change it at any time; if something breaks, please open an issue.
- Please respect each model's licence and the designers' work.

## Licence

[MIT](LICENSE)
