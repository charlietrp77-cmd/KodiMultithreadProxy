# Installation & Testing Guide

Step-by-step guide to installing and testing **Stream Accelerator (`service.stream.accelerator`)** together with **EasyNews Video (`plugin.video.easynewsx` v6.6.4)** in Kodi.

---

## Prerequisites

- **Kodi 19+ (Matrix / Nexus / Omega)** — Python 3 (`xbmc.python` 3.0.0)
- Enable **Unknown Sources** in Kodi:
  1. Open **Kodi Settings (gear icon)** → **System** → **Add-ons**
  2. Toggle **Unknown sources** **ON** and confirm **Yes**

---

## Step 1: Download the Installable Zips

Download both `.zip` files from the [`zips/`](zips/) folder in this repository (or use the local copies in `C:\KodiMultithreadProxy\zips\`):

1. [`zips/service.stream.accelerator.zip`](zips/service.stream.accelerator.zip) — Multi-Threaded Stream Accelerator service
2. [`zips/plugin.video.easynewsx.zip`](zips/plugin.video.easynewsx.zip) — EasyNews Video v6.6.4 (with Stream Accelerator integration)

> **Install order matters:** Install `service.stream.accelerator.zip` **first**, then `plugin.video.easynewsx.zip`, so Kodi immediately satisfies the dependency and adds the module to EasyNews's Python path.

---

## Step 2: Install in Kodi

1. Open Kodi → **Add-ons** (puzzle/box icon) → **Install from zip file**
2. Browse to and select **`service.stream.accelerator.zip`**
   - Wait for the **"Multi-Threaded Stream Accelerator Add-on installed"** notification.
3. Open **Install from zip file** again
4. Browse to and select **`plugin.video.easynewsx.zip`**
   - Wait for the **"EasyNews Video by Tetsuo Add-on updated/installed"** notification.
5. *(Recommended)* **Restart Kodi once** after initial installation so the background service starts cleanly on login and installs the hotkey keymap (`accelerator_keymap.xml`).

---

## Step 3: Enable Stream Accelerator in EasyNews

The accelerator is **opt-in** (disabled by default in EasyNews):

1. Go to **Add-ons** → **Video add-ons** → highlight **EasyNews Video by Tetsuo**
2. Open context menu (**C** or long-press / right-click) → **Settings**
3. Select the **Stream Accelerator** category
4. Turn **Enable Multi-Thread Accelerator** **ON**
5. Click **OK**

---

## Step 4: Configure & Debug Settings (Optional)

Open **Add-ons** → **My add-ons** → **Services** → **Multi-Threaded Stream Accelerator** → **Configure**:

| Setting | Default | Notes |
|---|---|---|
| **Local Proxy Port** | `48990` | Falls back to `48991` automatically if in use |
| **Worker Threads per Stream** | `6` | 4–8 recommended for Easynews; up to 12–16 for Debrid |
| **Chunk Size (KB)** | `1024` (1 MB) | Range: 256 KB – 8192 KB |
| **Max Read-Ahead Chunks in RAM** | `12` | 12 MB RAM at default chunk size; lower to 6–8 on low-RAM Android boxes |
| **Chunk Retry Attempts** | `3` | Retries per failed chunk before aborting |
| **Small Read Bypass Threshold (MB)** | `4` | Requests ≤ threshold skip threading (fast header/cue reads) |
| **Enable Stats Overlay Hotkey (Ctrl+L)** | `true` | Allows toggling the live OSD overlay |
| **Enable Verbose Logging** | `false` | Turn **ON** to write detailed diagnostics (probe timing, per-chunk throughput, slow-start, consumer stalls) directly to `kodi.log` **without** enabling Kodi's noisy system-wide debug overlay |

---

## Step 5: A/B Testing (On vs. Off) & Live Overlay

During playback, use these hotkeys to measure performance:

| Hotkey | Action |
|---|---|
| **`Ctrl+L`** | Toggle the live **Stats Overlay** (top-right corner). Press `Back` / `Esc` or `Ctrl+L` again to close. |
| **`Ctrl+Shift+L`** | Toggle proxy acceleration **ON / OFF** for the **next** stream (shows a Kodi notification). |

### Suggested A/B Comparison Test

1. Start an EasyNews video (ideally a large ≥2 GB 1080p/4K MKV).
2. Press **`Ctrl+L`** to open the overlay — verify it says `● PROXY ACTIVE — ACCELERATED` and check live speed, avg speed, worker activity, and buffer fill.
3. Let it play ~60 seconds and test seeking forward a few times. Stop playback.
4. Press **`Ctrl+Shift+L`** → notification shows **Proxy OFF — next stream goes direct**.
5. Start the **same** video again → press **`Ctrl+L`** to confirm `○ PROXY DISABLED` (or press `O` for Kodi's built-in codec/buffer OSD) and compare buffering/speed.
6. Press **`Ctrl+Shift+L`** again to switch back **ON**.

---

## Checking Logs (`kodi.log`)

If **Enable Verbose Logging** is turned **ON** in Stream Accelerator settings, inspect `kodi.log`:

- **Windows:** `%APPDATA%\Kodi\kodi.log`
- **Linux:** `~/.kodi/temp/kodi.log`
- **Android:** `/sdcard/Android/data/org.xbmc.kodi/files/.kodi/temp/kodi.log`

Filter for `[StreamAccelerator` or `plugin.video.easynewsx` to see:
- Upstream probe timings (`HEAD` / `GET` / `CACHE HIT`)
- Mode chosen (`ACCELERATED`, `BYPASS_SMALL`, or `DIRECT`)
- Chunk-0 slow-start & window expansion
- Per-chunk HTTP 206 latency and throughput (`Mbit/s`)
- Consumer stall warnings (`Consumer stalled Xms waiting for chunk N`) — useful for tuning `threads`, `chunk_size_kb`, and `read_ahead`!

