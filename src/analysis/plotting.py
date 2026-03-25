import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager, rcParams

def configure_matplotlib_for_japanese() -> str | None:
    candidate_keywords = [
        "Noto Sans CJK JP",
        "Noto Serif CJK JP",
        "IPAexGothic",
        "IPAGothic",
        "Yu Gothic",
        "YuGothic",
        "Hiragino Sans",
        "Hiragino Kaku Gothic",
        "MS Gothic",
        "TakaoGothic",
        "Source Han Sans JP",
        "Source Han Serif JP",
    ]

    available_fonts = font_manager.fontManager.ttflist
    chosen_font = None

    for keyword in candidate_keywords:
        for font in available_fonts:
            if keyword.lower() in font.name.lower():
                chosen_font = font.name
                break
        if chosen_font is not None:
            break

    if chosen_font is not None:
        rcParams["font.family"] = chosen_font

    rcParams["axes.unicode_minus"] = False
    rcParams["figure.dpi"] = 150
    rcParams["savefig.dpi"] = 150

    return chosen_font

def add_trend_line(x, y) -> None:
    import pandas as pd

    valid = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(valid) < 2:
        return

    z = np.polyfit(valid["x"], valid["y"], 1)
    p = np.poly1d(z)
    plt.plot(valid["x"], p(valid["x"]))