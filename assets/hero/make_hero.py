"""Build the README hero banner, assets/scherlok-hero.svg, as a self-contained animated SVG.

An SVG shown as an image cannot load web fonts, so the text is converted to
outlines: Space Grotesk for the wordmark and headline, JetBrains Mono for the
mono lines (both OFL, from github.com/google/fonts). The animation is CSS
inside the SVG: the glitch on "break.", the scanner in the lens, the pulsing
dot and the caret, all off under prefers-reduced-motion.

To rebuild it, outside the project's environment:

    python -m venv /tmp/hero && /tmp/hero/bin/pip install fonttools uharfbuzz
    mkdir -p /tmp/fonts && cd /tmp/fonts
    curl -sSLo SpaceGrotesk.ttf \\
        "https://github.com/google/fonts/raw/main/ofl/spacegrotesk/SpaceGrotesk%5Bwght%5D.ttf"
    curl -sSLo JetBrainsMono.ttf \\
        "https://github.com/google/fonts/raw/main/ofl/jetbrainsmono/JetBrainsMono%5Bwght%5D.ttf"
    cd - && /tmp/hero/bin/python assets/hero/make_hero.py /tmp/fonts assets/scherlok-hero.svg

The weight instances are cached in FONTS_DIR. The CRITICAL line is real
`scherlok demo` output (v1.5.0).
"""

from __future__ import annotations

import sys
from pathlib import Path

import uharfbuzz as hb
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

TEXT = "#E4EEEB"
MUTED = "#8D9DA8"
DIM = "#5B6B77"
NEON = "#2EF2C8"
CYAN = "#38D9F5"
VIOLET = "#8B6CFF"
RED = "#FF4D6D"
BG = "#05070B"
LINE = "rgba(130,255,225,0.22)"

WIDTH, HEIGHT, RADIUS = 1200, 480, 28
LEFT = 64

HEADLINE = ("Don’t wait for your data", "pipelines to ", "break.")
TAGLINE = "Scherlok: data reliability for the AI era."
TITLE = f"{''.join(HEADLINE)} {TAGLINE}"
DEMO_TAG = "CRITICAL"
DEMO_LINE = "  orders  Row count dropped 60.0% (20,000 -> 8,000)"

# The large lens on the right: centre and radius.
LENS_X, LENS_Y, LENS_R = 1036, 214, 94


class Face:
    """One weight of a font: shapes text with HarfBuzz and draws it as SVG path data."""

    def __init__(self, path: Path):
        self.tt = TTFont(path)
        self.glyphs = self.tt.getGlyphSet()
        self.upem = self.tt["head"].unitsPerEm
        self.hb = hb.Font(hb.Face(hb.Blob.from_file_path(str(path))))
        self.x_height = getattr(self.tt["OS/2"], "sxHeight", 0) or self.upem * 0.5

    def path(
        self,
        text: str,
        size: float,
        x: float,
        baseline: float,
        tracking_em: float = 0.0,
        ligatures: bool = True,
    ) -> tuple[str, float]:
        """Outline of `text` with its baseline at (x, baseline); returns (path data, end x)."""
        buf = hb.Buffer()
        buf.add_str(text)
        buf.guess_segment_properties()
        hb.shape(self.hb, buf, {"kern": True, "liga": ligatures, "calt": ligatures})
        scale = size / self.upem
        pen = SVGPathPen(self.glyphs)
        cursor = x
        for info, pos in zip(buf.glyph_infos, buf.glyph_positions):
            name = self.tt.getGlyphName(info.codepoint)
            origin = (cursor + pos.x_offset * scale, baseline - pos.y_offset * scale)
            self.glyphs[name].draw(TransformPen(pen, (scale, 0, 0, -scale, *origin)))
            cursor += pos.x_advance * scale + tracking_em * size
        return pen.getCommands(), cursor


def instance(fonts: Path, family: str, weight: int) -> Face:
    """A static instance of a variable font at `weight`, cached in `fonts`."""
    path = fonts / f"{family}-{weight}.ttf"
    if not path.exists():
        font = TTFont(fonts / f"{family}.ttf")
        instantiateVariableFont(font, {"wght": weight}, inplace=True)
        font.save(path)
    return Face(path)


def fill(color: str, d: str, **attrs: str) -> str:
    extra = "".join(f' {key.replace("_", "-")}="{value}"' for key, value in attrs.items())
    return f'<path fill="{color}" d="{d}"{extra}/>'


def wordmark(face: Face) -> str:
    """'scherlok' with the lens as the 'o', top left."""
    size, base, tracking = 34, 84, -0.045
    out = []
    d, x = face.path("scherl", size, LEFT, base, tracking)
    out.append(fill(TEXT, d))
    x_height = face.x_height * size / face.upem
    r, stroke = x_height * 0.42, x_height * 0.21
    cx, cy = x + r + stroke * 0.6, base - x_height / 2
    h1 = (cx + r * 0.72, cy + r * 0.72)
    h2 = (h1[0] + x_height * 0.34, h1[1] + x_height * 0.34)
    out.append(
        f'<g fill="none" stroke="{NEON}" stroke-linecap="round">'
        f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="{r:.2f}" stroke-width="{stroke:.2f}"/>'
        f'<path d="M{h1[0]:.2f} {h1[1]:.2f} L{h2[0]:.2f} {h2[1]:.2f}" '
        f'stroke-width="{stroke * 1.06:.2f}"/></g>'
        f'<circle class="ping" cx="{cx:.2f}" cy="{cy:.2f}" r="{r * 0.36:.2f}" fill="{RED}"/>'
    )
    d, _ = face.path("k", size, cx + r + stroke * 0.25, base, tracking)
    out.append(fill(TEXT, d))
    return "".join(out)


def headline(face: Face) -> str:
    """Two lines; 'break.' in neon with a glow and three glitch slices."""
    size, tracking = 74, -0.022
    first, second, accent = HEADLINE
    d1, _ = face.path(first, size, LEFT - 3, 212, tracking)
    d2, x = face.path(second, size, LEFT - 3, 300, tracking)
    d3, _ = face.path(accent, size, x, 300, tracking)
    # Defined once without a colour, so each copy below sets its own.
    return (
        fill(TEXT, d1)
        + fill(TEXT, d2)
        + f'<defs><path id="brk" d="{d3}"/></defs>'
        + f'<g class="gc"><use href="#brk" fill="{RED}" clip-path="url(#band-mid)"/></g>'
        + f'<use href="#brk" fill="{NEON}" filter="url(#glow)"/>'
        + f'<g class="ga"><use href="#brk" fill="{RED}" clip-path="url(#band-top)"/></g>'
        + f'<g class="gb"><use href="#brk" fill="{CYAN}" clip-path="url(#band-low)"/></g>'
    )


def mono_lines(face: Face) -> str:
    """The tagline, then a real line of `scherlok demo` output with a blinking caret."""
    out = []
    d, x = face.path("> ", 22, LEFT, 362)
    out.append(fill(DIM, d))
    d, _ = face.path(TAGLINE, 22, x, 362)
    out.append(fill(CYAN, d))
    base = 424
    d, x = face.path(DEMO_TAG, 16, LEFT, base)
    out.append(fill(RED, d))
    d, x = face.path(DEMO_LINE, 16, x, base, ligatures=False)
    out.append(fill(MUTED, d))
    out.append(
        f'<rect class="caret" x="{x + 8:.1f}" y="{base - 13}" width="9" height="16" '
        f'fill="{NEON}"/>'
    )
    return "".join(out)


def lens() -> str:
    """A large translucent lens on the right, with a scanner sweeping inside it."""
    x, y, r = LENS_X, LENS_Y, LENS_R
    return (
        f'<g opacity=".22" fill="none" stroke="{NEON}" stroke-linecap="round">'
        f'<circle cx="{x}" cy="{y}" r="{r}" stroke-width="16"/>'
        f'<path d="M{x + r * 0.72:.1f} {y + r * 0.72:.1f} '
        f'L{x + r * 1.42:.1f} {y + r * 1.42:.1f}" stroke-width="26"/></g>'
        '<g clip-path="url(#lens-in)">'
        f'<rect class="scan" x="{x - r}" y="{y - r}" width="{r * 2}" height="3" '
        f'fill="{NEON}" opacity=".85" filter="url(#glow)"/></g>'
        f'<circle class="ping" cx="{x}" cy="{y}" r="22" fill="{RED}" opacity=".9"/>'
    )


STYLE = "\n".join([
    ".ga,.gb,.gc{opacity:0}",
    ".ga{animation:ga 4.5s infinite steps(1)}",
    ".gb{animation:gb 4.5s infinite steps(1)}",
    ".gc{animation:gc 4.5s infinite steps(1)}",
    "@keyframes ga{0%,85%,100%{opacity:0;transform:none}"
    "86%{opacity:.95;transform:translate(-5px,1px)}"
    "89%{opacity:.95;transform:translate(5px,-1px)}92%{opacity:0}}",
    "@keyframes gb{0%,86%,100%{opacity:0;transform:none}"
    "87%{opacity:.95;transform:translate(5px,-1px)}"
    "90%{opacity:.95;transform:translate(-4px,1px)}93%{opacity:0}}",
    "@keyframes gc{0%,87%,100%{opacity:0;transform:none}"
    "88%{opacity:.9;transform:translate(-3px,0)}"
    "91%{opacity:.9;transform:translate(3px,0)}94%{opacity:0}}",
    ".caret{animation:blink 1s steps(1) infinite}",
    "@keyframes blink{50%{opacity:0}}",
    ".ping{animation:ping 2.4s ease-in-out infinite}",
    "@keyframes ping{50%{opacity:.35}}",
    ".scan{animation:scan 4s ease-in-out infinite}",
    f"@keyframes scan{{0%,100%{{transform:translateY(8px)}}"
    f"50%{{transform:translateY({LENS_R * 2 - 10}px)}}}}",
    "@media (prefers-reduced-motion: reduce){.ga,.gb,.gc,.caret,.ping,.scan{animation:none}}",
])


def defs() -> str:
    def stops(color: str, opacity: str) -> str:
        return (
            f'<stop offset="0" stop-color="{color}" stop-opacity="{opacity}"/>'
            f'<stop offset="1" stop-color="{color}" stop-opacity="0"/>'
        )

    def band(name: str, y: int, height: int) -> str:
        rect = f'<rect x="0" y="{y}" width="{WIDTH}" height="{height}"/>'
        return f'<clipPath id="{name}">{rect}</clipPath>'

    return "\n".join([
        '<pattern id="grid" width="48" height="48" patternUnits="userSpaceOnUse">'
        '<path d="M48 0H0V48" fill="none" stroke="rgba(130,255,225,0.09)"/></pattern>',
        f'<radialGradient id="fade" cx="35%" cy="0%" r="85%">{stops("#fff", "1")}</radialGradient>',
        f'<mask id="grid-mask"><rect width="{WIDTH}" height="{HEIGHT}" fill="url(#fade)"/></mask>',
        f'<radialGradient id="orb-a">{stops("#0E8F78", ".55")}</radialGradient>',
        f'<radialGradient id="orb-b">{stops(VIOLET, ".38")}</radialGradient>',
        f'<clipPath id="lens-in"><circle cx="{LENS_X}" cy="{LENS_Y}" r="{LENS_R - 9}"/></clipPath>',
        f'<clipPath id="frame"><rect width="{WIDTH}" height="{HEIGHT}" rx="{RADIUS}"/></clipPath>',
        band("band-top", 236, 24),
        band("band-mid", 262, 14),
        band("band-low", 278, 22),
        '<filter id="glow" x="-10%" y="-30%" width="120%" height="160%">'
        '<feGaussianBlur stdDeviation="7" result="b"/>'
        '<feColorMatrix in="b" type="matrix" '
        'values="0 0 0 0 0.18  0 0 0 0 0.95  0 0 0 0 0.78  0 0 0 .55 0"/>'
        '<feMerge><feMergeNode/><feMergeNode in="SourceGraphic"/></feMerge></filter>',
    ])


def build(fonts: Path) -> str:
    grotesk_600 = instance(fonts, "SpaceGrotesk", 600)
    grotesk_700 = instance(fonts, "SpaceGrotesk", 700)
    mono_500 = instance(fonts, "JetBrainsMono", 500)
    body = "".join([
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="{BG}"/>',
        '<circle cx="120" cy="40" r="420" fill="url(#orb-a)"/>',
        '<circle cx="1120" cy="200" r="460" fill="url(#orb-b)"/>',
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="url(#grid)" mask="url(#grid-mask)"/>',
        lens(),
        wordmark(grotesk_700),
        headline(grotesk_600),
        mono_lines(mono_500),
    ])
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'width="{WIDTH}" height="{HEIGHT}" role="img" aria-labelledby="t">\n'
        f'<title id="t">{TITLE}</title>\n'
        f"<style>\n{STYLE}\n</style>\n"
        f"<defs>\n{defs()}\n</defs>\n"
        f'<g clip-path="url(#frame)">{body}</g>\n'
        f'<rect x="0.5" y="0.5" width="{WIDTH - 1}" height="{HEIGHT - 1}" rx="{RADIUS - 0.5}" '
        f'fill="none" stroke="{LINE}"/>\n'
        "</svg>\n"
    )


def main() -> None:
    fonts, out = Path(sys.argv[1]), Path(sys.argv[2])
    svg = build(fonts)
    out.write_text(svg)
    print(f"wrote {out} ({len(svg) // 1024} KB)")


if __name__ == "__main__":
    main()
