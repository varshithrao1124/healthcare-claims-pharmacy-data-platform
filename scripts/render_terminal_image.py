"""Render a real text log into a PNG so the README can show the run.

The pixels come from the captured stdout/stderr file. This is not a mock of
the Azure portal.
"""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def font(size: int):
    path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf")
    if path.exists():
        return ImageFont.truetype(str(path), size)
    return ImageFont.load_default()


def render(source: Path, dest: Path, title: str, max_lines: int = 48) -> None:
    lines = source.read_text(encoding="utf-8", errors="replace").splitlines()
    # Keep the tail, which holds the summary, and drop Spark progress noise.
    cleaned = [line for line in lines if "Stage " not in line and not line.startswith("\r")]
    if len(cleaned) > max_lines:
        cleaned = cleaned[-max_lines:]
    fnt = font(15)
    width = 1400
    line_h = 22
    height = 70 + line_h * (len(cleaned) + 1)
    image = Image.new("RGB", (width, height), "#0d1117")
    draw = ImageDraw.Draw(image)
    draw.text((20, 16), title, fill="#8b949e", font=fnt)
    y = 50
    for line in cleaned:
        draw.text((20, y), line[:160], fill="#c9d1d9", font=fnt)
        y += line_h
    dest.parent.mkdir(parents=True, exist_ok=True)
    image.save(dest)
    print(dest)


if __name__ == "__main__":
    render(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
