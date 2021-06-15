"""
colors & style for data_irl
See doc:
https://docs.google.com/document/d/1Kyq9z5eaqpWumgpZw8O9W8yOki8gCGSf3HHR5Yc8Jhk/edit#

Python notebook in Mode:
https://app.mode.com/editor/reddit/reports/53d41e56a17f/notebook
"""
from typing import Union

# import pandas as pd
import numpy as np
# from datetime import datetime
# import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter  # , date2num
import matplotlib.ticker as ticker
import matplotlib.colors as colors
# import matplotlib.pyplot as plt


def get_colormap(
        n_items,
        type='discrete',
        return_as_list=False
) -> Union[colors.ListedColormap, colors.LinearSegmentedColormap, list]:
    """
    Get data-irl palette based on n_items to plot. Max number is 8

    Args:
        n_items: how many items to plot?
        type: One of: [discrete, continuous, divergent, continuous_dark]
        return_as_list:
            Most of matplotlib is ok with a "cmap" object, but seaborn doesn't play well with them.
            Instead set return_as_list=True so that sns can interpret the function's output.

    Returns:
        a CMAP object (color.ListedColormap or color.LinearSegmentedColormap) or a list of colors
    """
    if type == 'discrete':
        reddit_base_cmap = np.array(
            ['#FF4500', '#0DD3BB', '#24A0ED', '#FFCA00', '#FFB000', '#FF8717', '#00A6A5', '#0079D3'])
        n_base_cmap = 8
        color_item_mapping = {
            1: [0],
            2: [0, 7],
            3: [0, 1, 7],
            4: [0, 1, 7, 4],
            5: [0, 1, 7, 4, 6],
            6: [0, 1, 7, 4, 6, 5],
            7: [0, 1, 7, 4, 6, 5, 2],
            8: [0, 1, 7, 4, 6, 5, 2, 3]
        }

        if not n_items:
            n_items = n_base_cmap

        if type == 'discrete' and n_items > n_base_cmap:
            raise Exception('Number of requested colors greater than what palette can offer')

        color_list = reddit_base_cmap[color_item_mapping[n_items]]

        if return_as_list:
            return color_list
        else:
            return colors.ListedColormap(color_list)

    elif type == 'continuous':
        return colors.LinearSegmentedColormap.from_list(None, ['#FF4500', '#fcece6'], N=n_items)
    elif type == 'divergent':
        return colors.LinearSegmentedColormap.from_list(None, ['#FF4500', '#f5f5f5', '#0079D3'], N=n_items)
    elif type == 'continuous_dark':
        return colors.LinearSegmentedColormap.from_list(None, ['#000000', '#FF4500', '#FFFFFF'], N=n_items)


def theme_dirl(ax, format_dates=False, format_pct=False, format_usd=False, zero_ylim=False):
    ax.set_facecolor('white')
    ax.set_axisbelow(True)
    ax.set_frame_on(False)
    if format_pct:
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: '{:.0%}'.format(x)))
    elif format_usd:
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: '${:.2}'.format(x)))
    else:
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    if zero_ylim:
        ax.set_ylim(0)
    ax.tick_params(labelsize=18)

    ax.legend(
        loc='center left',
        bbox_to_anchor=(1., 0.5),
        shadow=False,
        ncol=1,
        frameon=False,
        fontsize='x-large'
    )

    if format_dates:
        ax.xaxis_date()
        ax.tick_params(labelsize=18)
        ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
    else:
        ax.set_xticklabels(ax.get_xticklabels(), rotation='horizontal', fontsize=18)


def create_stacked_bar(df, ax, cmap):
    bottom = []
    plts = []
    for idx, col in enumerate(df.columns):
        if idx == 0:
            ax.bar(df.index, df[col], width=0.9, color=cmap.colors[idx], label=col)
            bottom = df[col]
        else:
            ax.bar(df.index, df[col], width=0.9, color=cmap.colors[idx], bottom=bottom, label=col)
            bottom = bottom + df[col]
    # ax.legend(handles=plts, labels=df.columns)


def get_color_dict(
        name: str = 'dirl',
) -> dict:
    """dict with named colors for easier tweaking, includes plotly colors

    To get plotly colors:
    - https://plotly.com/python/discrete-color/
    - px.colors.qualitative.Plotly
    - px.colors.qualitative.swatches()
    """
    d_reddit = {
        'orange_red': '#ff4500',
        'mint': '#0dd3bb',
        'blue': '#24a0ed',
        'yellow': '#ffca00',
        'mango': '#ffb000',
        'orange': '#ff8717',
        'teal': '#00a6a5',
        'blue_alien': '#0079d3',
    }

    d_plotly = {
        'blue_purple': '#636EFA',
        'orange': '#EF553B',
        'mint': '#00CC96',
        'purple_light': '#AB63FA',
        'orange_light': '#FFA15A',
        'blue_sky': '#19D3F3',
        'magenta': '#FF6692',
        'green_lime': '#B6E880',
        'pink': '#FF97FF',
        'yellow': '#FECB52',
    }

    if name == 'dirl':
        return d_reddit
    elif name == 'plotly':
        return d_plotly
    else:
        raise NotImplementedError


def base_colors_for_manual_labels() -> dict:
    """Base colors to map manual label to a color"""
    return {
        'reddit institutions': get_color_dict('dirl')['orange_red'],
        'internet culture and memes': get_color_dict('dirl')['orange'],
        'podcasts and streamers': get_color_dict('dirl')['yellow'],

        'place/culture': get_color_dict('plotly')['blue_sky'],

        'business, economics, and finance': get_color_dict('plotly')['purple_light'],
        'culture, entertainment, music': get_color_dict('dirl')['blue'],

        'world news': get_color_dict('plotly')['pink'],
        'technology': get_color_dict('dirl')['mango'],

        'gaming': get_color_dict('plotly')['green_lime'],
        'sports': get_color_dict('dirl')['teal'],

        'culture, race, and ethnicity': get_color_dict('dirl')['mint'],
        'food and drink': get_color_dict('dirl')['blue_alien'],
        'home and garden': get_color_dict('plotly')['magenta'],

        'uncategorized': "#e3dbda",  # light-reddish gray (too light: fcf3f2)
        'over18_nsfw': "#8c8c8c",  # dark gray
    }

def check_colors_used(
        d_new: dict,
        d_reference: dict,
):
    """Check which values have been used in one dictionary"""
    set_new = set(d_new.values())
    set_ref = set(d_reference.values())
    unused_vals = set_ref - set_new

    print(f"{len(d_new.keys())}  Total keys in d_new")
    print(f"{len(set_new)}  Unique values in d_new")

    l_colors_unused = [k for k, v in d_reference.items() if v in unused_vals]
    print(f"Unused keys from d_ref:\n  {l_colors_unused}")


#
# ~ fin
#
