"""
Add templates for common plots
"""

import numpy as np
import pandas as pd
import plotly.express as px


def px_scatter_3d_subreddits(
        df: pd.DataFrame,
        col_clustering: str = 'cluster_id_agg_ward_cosine_35',
        col_manual_labels: str = 'manual_topic_and_rating',
        l_custom_data_subs: list = None,
        size_col: str = 'users_l28',
):
    """display subreddits, primary use case closest subreddits to an input sub

    If more than 20 subs, the text title will be too noisy.
    """
    if l_custom_data_subs is None:
        l_custom_data_subs = [
            'subreddit_name',
            col_manual_labels,
            # 'text_1',
            # 'text_2',
        ]
    sub_hover_data = "<br>".join([
        "subreddit name: %{customdata[0]}",
        "subreddit manual label: %{customdata[1]}",
        # "post text: %{customdata[2]}",
        # "  %{customdata[3]}"
    ])
    fig = px.scatter_3d(
        df,
        y='svd_0', x='svd_1', z='svd_2',
        color=col_clustering,  # color=col_manual_labels,
        custom_data=l_custom_data_subs,
        size=np.log2(1 + df[size_col]),
        text='subreddit_name',
        # hoverinfo='text',
    )

    fig.update_traces(hovertemplate=sub_hover_data)
    fig.update_layout(
        title_text=(
            # f"Most similar subreddits to <i>r/{sub_}</i>"
            f"{len(df):,.0f} German-relevant subreddits"
            # f"<br>Clustering algo: {c_name.replace('cluster_id', '').replace('_', ' ')}"
            # f"<br>Using posts from 04-01-2021 to 05-08-2021"
        ),
        title_x=0.5,
        width=800,
        height=600,
        #     uniformtext_minsize=8, uniformtext_mode='hide'
    )
    # fig.show(renderer='png')
    # fig.show()
    return fig
