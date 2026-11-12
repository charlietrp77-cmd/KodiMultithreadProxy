"""Script entry point for RunScript() hotkey actions.

Called via:
  RunScript(service.stream.accelerator, overlay)   → Ctrl+D
  RunScript(service.stream.accelerator, toggle)    → Ctrl+Shift+D

This file is registered as xbmc.python.script in addon.xml.
"""
import sys


def main():
    if len(sys.argv) < 2:
        return

    command = sys.argv[1].strip().lower()

    if command == "overlay":
        from resources.lib.overlay import toggle_overlay
        toggle_overlay()
    elif command == "toggle":
        from resources.lib.overlay import toggle_proxy
        toggle_proxy()


if __name__ == "__main__":
    main()

