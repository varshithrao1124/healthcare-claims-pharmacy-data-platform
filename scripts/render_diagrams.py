"""Render architecture and ERD diagrams to SVG and PNG.

The Mermaid source lives in docs/architecture.md and docs/data-model.md.
These images are drawn from the same boxes and relationships so the README
can show them without a Mermaid renderer.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).resolve().parents[1] / "docs" / "diagrams"
OUT.mkdir(parents=True, exist_ok=True)


def font(size: int) -> ImageFont.ImageFont:
    for candidate in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ):
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size)
    return ImageFont.load_default()


def box(draw: ImageDraw.ImageDraw, xy, text, fill, outline="#1f4e79"):
    draw.rounded_rectangle(xy, radius=8, fill=fill, outline=outline, width=2)
    x0, y0, x1, y1 = xy
    lines = text.split("\n")
    fnt = font(14)
    total_h = len(lines) * 18
    y = y0 + (y1 - y0 - total_h) / 2
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=fnt)
        width = bbox[2] - bbox[0]
        draw.text((x0 + (x1 - x0 - width) / 2, y), line, fill="#102a43", font=fnt)
        y += 18


def arrow(draw, start, end):
    draw.line([start, end], fill="#486581", width=2)


def architecture_png() -> None:
    image = Image.new("RGB", (1400, 820), "#f7f9fb")
    draw = ImageDraw.Draw(image)
    title = font(22)
    draw.text(
        (40, 24),
        "Healthcare claims medallion — local path and Azure path",
        fill="#102a43",
        font=title,
    )
    draw.text(
        (40, 56),
        "Solid boxes run locally and in CI. Dashed labels are deploy-ready and not executed here.",
        fill="#486581",
        font=font(14),
    )

    box(draw, (40, 120, 250, 200), "Synthetic generator\nseeded CSV batches", "#e3f2fd")
    box(draw, (290, 120, 520, 200), "Landing zone\nbatch_date=yyyy-MM-dd", "#e3f2fd")
    box(draw, (560, 100, 820, 180), "Bronze Delta\nall-string + metadata", "#fff3e0")
    box(draw, (560, 210, 820, 290), "Silver Delta\nDQ, SCD2, MERGE", "#e8f5e9")
    box(draw, (560, 320, 820, 400), "Gold Delta\nstar schema marts", "#f3e5f5")
    box(draw, (880, 160, 1140, 250), "Audit\nruns, DQ, reconciliation", "#fce4ec")
    box(draw, (880, 300, 1140, 390), "Local dashboard\nStreamlit on Gold", "#ede7f6")
    box(draw, (1180, 300, 1360, 390), "Power BI\n(design only)", "#ede7f6")

    arrow(draw, (250, 160), (290, 160))
    arrow(draw, (520, 160), (560, 140))
    arrow(draw, (690, 180), (690, 210))
    arrow(draw, (690, 290), (690, 320))
    arrow(draw, (820, 150), (880, 190))
    arrow(draw, (820, 250), (880, 210))
    arrow(draw, (820, 360), (880, 340))
    arrow(draw, (1140, 345), (1180, 345))

    box(
        draw,
        (40, 480, 320, 600),
        "ADF copy\nlanding to ADLS raw\nKey Vault linked service",
        "#eceff1",
    )
    box(draw, (360, 480, 660, 600), "Auto Loader\ncloudFiles + rescued data", "#eceff1")
    box(draw, (700, 480, 980, 600), "Lakeflow / DLT\nalternate Silver expectations", "#eceff1")
    box(draw, (1020, 480, 1340, 600), "Unity Catalog\nBicep + Asset Bundle", "#eceff1")
    draw.text(
        (40, 640),
        "Azure deployment path (not executed in this environment)",
        fill="#486581",
        font=font(16),
    )
    arrow(draw, (320, 540), (360, 540))
    arrow(draw, (660, 540), (700, 540))
    arrow(draw, (980, 540), (1020, 540))
    image.save(OUT / "architecture.png")


def erd_png() -> None:
    image = Image.new("RGB", (1400, 780), "#f7f9fb")
    draw = ImageDraw.Draw(image)
    draw.text((40, 24), "Gold star schema (payer analytics)", fill="#102a43", font=font(22))
    dims = [
        (40, 100, "dim_member\nmember_id"),
        (280, 100, "dim_provider\nprovider_npi\nSCD2 history"),
        (540, 100, "dim_pharmacy\npharmacy_npi\nSCD2 history"),
        (800, 100, "dim_drug\nndc"),
        (1040, 100, "dim_drug_pricing\nndc + price_type\nSCD2"),
        (40, 280, "dim_date\ndate_key"),
        (280, 280, "dim_denial_reason\ndenial_reason_code"),
    ]
    for x, y, text in dims:
        box(draw, (x, y, x + 210, y + 110), text, "#e3f2fd")
    box(
        draw,
        (80, 470, 460, 620),
        "fact_medical_claim\nclaim_id + line\nmember, provider, date",
        "#fff3e0",
    )
    box(
        draw,
        (520, 470, 980, 640),
        "fact_pharmacy_claim\nclaim_id + line\nmember, pharmacy, prescriber, ndc, price",
        "#e8f5e9",
    )
    box(
        draw,
        (1020, 470, 1340, 640),
        "Marts\nPMPM, denial, GDR\ntop drugs, variance\nprescribing, utilization",
        "#f3e5f5",
    )
    arrow(draw, (145, 210), (200, 470))
    arrow(draw, (385, 210), (300, 470))
    arrow(draw, (645, 210), (700, 470))
    arrow(draw, (905, 210), (760, 470))
    arrow(draw, (1145, 210), (800, 470))
    arrow(draw, (980, 560), (1020, 560))
    image.save(OUT / "erd.png")


def architecture_svg() -> None:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="820" viewBox="0 0 1400 820">
  <rect width="1400" height="820" fill="#f7f9fb"/>
  <text x="40" y="48" font-family="sans-serif" font-size="22" fill="#102a43">Healthcare claims medallion — local path and Azure path</text>
  <text x="40" y="76" font-family="sans-serif" font-size="14" fill="#486581">Solid flow runs locally and in CI. The lower row is deploy-ready and was not executed against Azure.</text>
  <g font-family="sans-serif" font-size="14" fill="#102a43">
    <rect x="40" y="120" width="220" height="80" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="150" y="155" text-anchor="middle">Synthetic generator</text>
    <text x="150" y="175" text-anchor="middle">seeded CSV batches</text>
    <rect x="300" y="120" width="230" height="80" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="415" y="155" text-anchor="middle">Landing zone</text>
    <text x="415" y="175" text-anchor="middle">batch_date partitions</text>
    <rect x="580" y="90" width="250" height="70" rx="8" fill="#fff3e0" stroke="#1f4e79"/>
    <text x="705" y="130" text-anchor="middle">Bronze Delta</text>
    <rect x="580" y="190" width="250" height="70" rx="8" fill="#e8f5e9" stroke="#1f4e79"/>
    <text x="705" y="230" text-anchor="middle">Silver Delta (DQ, SCD2, MERGE)</text>
    <rect x="580" y="290" width="250" height="70" rx="8" fill="#f3e5f5" stroke="#1f4e79"/>
    <text x="705" y="330" text-anchor="middle">Gold star schema</text>
    <rect x="880" y="150" width="250" height="80" rx="8" fill="#fce4ec" stroke="#1f4e79"/>
    <text x="1005" y="185" text-anchor="middle">Audit and reconciliation</text>
    <text x="1005" y="205" text-anchor="middle">runs, DQ, alerts</text>
    <rect x="880" y="290" width="220" height="70" rx="8" fill="#ede7f6" stroke="#1f4e79"/>
    <text x="990" y="330" text-anchor="middle">Local Streamlit dashboard</text>
    <rect x="1140" y="290" width="210" height="70" rx="8" fill="#ede7f6" stroke="#1f4e79"/>
    <text x="1245" y="330" text-anchor="middle">Power BI (design)</text>
    <rect x="40" y="500" width="280" height="100" rx="8" fill="#eceff1" stroke="#1f4e79"/>
    <text x="180" y="545" text-anchor="middle">ADF copy to ADLS raw</text>
    <text x="180" y="565" text-anchor="middle">Key Vault linked service</text>
    <rect x="360" y="500" width="280" height="100" rx="8" fill="#eceff1" stroke="#1f4e79"/>
    <text x="500" y="545" text-anchor="middle">Databricks Auto Loader</text>
    <text x="500" y="565" text-anchor="middle">cloudFiles, rescued data</text>
    <rect x="680" y="500" width="300" height="100" rx="8" fill="#eceff1" stroke="#1f4e79"/>
    <text x="830" y="545" text-anchor="middle">Lakeflow / DLT expectations</text>
    <text x="830" y="565" text-anchor="middle">alternate Silver path</text>
    <rect x="1020" y="500" width="320" height="100" rx="8" fill="#eceff1" stroke="#1f4e79"/>
    <text x="1180" y="545" text-anchor="middle">Unity Catalog, Bicep,</text>
    <text x="1180" y="565" text-anchor="middle">Asset Bundle, GitHub CD</text>
  </g>
  <g stroke="#486581" stroke-width="2" fill="none">
    <line x1="260" y1="160" x2="300" y2="160"/>
    <line x1="530" y1="160" x2="580" y2="125"/>
    <line x1="705" y1="160" x2="705" y2="190"/>
    <line x1="705" y1="260" x2="705" y2="290"/>
    <line x1="830" y1="125" x2="880" y2="180"/>
    <line x1="830" y1="225" x2="880" y2="200"/>
    <line x1="830" y1="325" x2="880" y2="325"/>
    <line x1="1100" y1="325" x2="1140" y2="325"/>
    <line x1="320" y1="550" x2="360" y2="550"/>
    <line x1="640" y1="550" x2="680" y2="550"/>
    <line x1="980" y1="550" x2="1020" y2="550"/>
  </g>
</svg>
"""
    (OUT / "architecture.svg").write_text(svg, encoding="utf-8")


def erd_svg() -> None:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="760" viewBox="0 0 1400 760">
  <rect width="1400" height="760" fill="#f7f9fb"/>
  <text x="40" y="48" font-family="sans-serif" font-size="22" fill="#102a43">Gold star schema</text>
  <g font-family="sans-serif" font-size="14" fill="#102a43">
    <rect x="40" y="90" width="200" height="90" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="140" y="130" text-anchor="middle">dim_member</text>
    <text x="140" y="150" text-anchor="middle">PK member_id</text>
    <rect x="280" y="90" width="220" height="110" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="390" y="125" text-anchor="middle">dim_provider</text>
    <text x="390" y="145" text-anchor="middle">PK provider_npi</text>
    <text x="390" y="165" text-anchor="middle">SCD2 history</text>
    <rect x="540" y="90" width="220" height="110" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="650" y="125" text-anchor="middle">dim_pharmacy</text>
    <text x="650" y="145" text-anchor="middle">PK pharmacy_npi</text>
    <text x="650" y="165" text-anchor="middle">SCD2 history</text>
    <rect x="800" y="90" width="180" height="90" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="890" y="130" text-anchor="middle">dim_drug</text>
    <text x="890" y="150" text-anchor="middle">PK ndc</text>
    <rect x="1020" y="90" width="240" height="110" rx="8" fill="#e3f2fd" stroke="#1f4e79"/>
    <text x="1140" y="125" text-anchor="middle">dim_drug_pricing</text>
    <text x="1140" y="145" text-anchor="middle">PK ndc + price_type</text>
    <text x="1140" y="165" text-anchor="middle">SCD2 AWP/WAC/MAC</text>
    <rect x="80" y="460" width="420" height="140" rx="8" fill="#fff3e0" stroke="#1f4e79"/>
    <text x="290" y="510" text-anchor="middle">fact_medical_claim</text>
    <text x="290" y="535" text-anchor="middle">grain: claim_id + claim_line_number</text>
    <text x="290" y="555" text-anchor="middle">FK member_id, billing/rendering NPI</text>
    <rect x="540" y="460" width="480" height="160" rx="8" fill="#e8f5e9" stroke="#1f4e79"/>
    <text x="780" y="510" text-anchor="middle">fact_pharmacy_claim</text>
    <text x="780" y="535" text-anchor="middle">grain: claim_id + claim_line_number</text>
    <text x="780" y="555" text-anchor="middle">FK member, pharmacy, prescriber, ndc</text>
    <text x="780" y="575" text-anchor="middle">contracted price and variance</text>
    <rect x="1060" y="460" width="290" height="160" rx="8" fill="#f3e5f5" stroke="#1f4e79"/>
    <text x="1205" y="500" text-anchor="middle">Aggregate marts</text>
    <text x="1205" y="525" text-anchor="middle">fact_pmpm</text>
    <text x="1205" y="545" text-anchor="middle">fact_denial_*</text>
    <text x="1205" y="565" text-anchor="middle">fact_generic_dispensing</text>
    <text x="1205" y="585" text-anchor="middle">fact_top_drugs, variance</text>
  </g>
</svg>
"""
    (OUT / "erd.svg").write_text(svg, encoding="utf-8")


def main() -> None:
    architecture_png()
    erd_png()
    architecture_svg()
    erd_svg()
    print(f"Wrote diagrams to {OUT}")


if __name__ == "__main__":
    main()
