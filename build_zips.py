"""Build installable Kodi addon zips for both addons."""
import os
import zipfile
import hashlib

def zip_addon(source_dir, addon_id, output_dir):
    """Create a Kodi-installable zip from an addon directory."""
    zip_name = f"{addon_id}.zip"
    zip_path = os.path.join(output_dir, zip_name)

    if os.path.exists(zip_path):
        os.remove(zip_path)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            # Skip __pycache__ and .git
            dirs[:] = [d for d in dirs if d not in ('__pycache__', '.git', '__MACOSX')]
            for f in files:
                if f.endswith('.pyc') or f.startswith('.'):
                    continue
                filepath = os.path.join(root, f)
                arcname = os.path.join(addon_id, os.path.relpath(filepath, source_dir))
                zf.write(filepath, arcname)

    size = os.path.getsize(zip_path)
    md5 = hashlib.md5(open(zip_path, 'rb').read()).hexdigest()
    print(f"  Created: {zip_path}")
    print(f"  Size: {size:,} bytes")
    print(f"  MD5: {md5}")
    return zip_path


if __name__ == "__main__":
    output = r"C:\KodiMultithreadProxy\zips"
    os.makedirs(output, exist_ok=True)

    print("=== Building service.stream.accelerator ===")
    zip_addon(
        r"C:\KodiMultithreadProxy\service.stream.accelerator",
        "service.stream.accelerator",
        output
    )

    print("\n=== Building plugin.video.easynewsx ===")
    zip_addon(
        r"C:\EasyNewsKodi\plugin.video.easynewsx",
        "plugin.video.easynewsx",
        output
    )

    print(f"\nAll zips ready in: {output}")
    print("Install in Kodi: Settings > Add-ons > Install from zip file")

