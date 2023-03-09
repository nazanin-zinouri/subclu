# Python Notebook - [i18n] Popular Subreddits in your City [v0]

datasets

import numpy as np
import pandas as pd

import plotly
import plotly.express as px

df_city_and_loc = datasets['04 city coordinates'].copy()

df_city_and_loc['city_users_l7_scaled'] = pd.cut(
    df_city_and_loc['city_users_l7'],
    bins=[
        -1, df_city_and_loc['city_users_l7'].quantile(0.20),
        df_city_and_loc['city_users_l7'].quantile(0.40),
        df_city_and_loc['city_users_l7'].quantile(0.55),
        df_city_and_loc['city_users_l7'].quantile(0.65),
        df_city_and_loc['city_users_l7'].quantile(0.75),
        df_city_and_loc['city_users_l7'].quantile(0.85),
        df_city_and_loc['city_users_l7'].quantile(0.97),
        df_city_and_loc['city_users_l7'].quantile(0.990)
        , np.inf
    ],
    labels=[0.7, 0.9, 1.1, 1.2, 1.6, 2.4, 3.8, 9, 20]
)
print(df_city_and_loc['city_users_l7_scaled'].describe())
# value_counts_and_pcts(df_city_and_loc['city_users_l7_scaled'], sort_index=True, sort_index_ascending=True)
df_city_and_loc['city_users_l7_scaled'].value_counts()

df_city_and_loc.head()

df_top_city = datasets['01 Top Subreddits by CITY rank'].copy()
print(df_top_city.shape)
df_top_city.head()

# get initial list
df_top_city_agg = (
    df_top_city
    .groupby(['country_code', 'region', 'city'])
    .agg(
        **{
            'top_subreddits': ('subreddit_name', list)
        }
    )
)
# split list with <br> to make display easier/better
df_top_city_agg['top_subreddits'] = (
    df_top_city_agg['top_subreddits']
    .apply(lambda x: '<br> r/'.join(x))
    .astype(str)
    .str.replace("'", "")
)

df_top_city_agg.head()

df_city_map = df_top_city_agg.copy().merge(
    df_city_and_loc.rename(columns={'geo_country_code': 'country_code', 'geo_region': 'region', 'geo_city': 'city'}),
    how='left',
    on=['country_code', 'region', 'city'],
)
df_city_map.shape

df_city_map.head()

l_top_country_codes = [
  "DE", "MX", "AU"
  , "FR", "NL", "IT", "ES", "BR"
  , "US", "GB", "IN", "CA"
]

df_city_and_loc[(
    # top from target countries
    (
        (df_city_and_loc['geo_country_code'].isin(l_top_country_codes)) &
        (df_city_and_loc['city_rank_country'] == 1)
    ) |
    # top from the US
    (
        (df_city_and_loc['geo_country_code'] == 'US') &
        (df_city_and_loc['city_rank_country'] <= 3)
    )
)]

# # add TOP city annotations separately. This way text should always be on top.
# # NVM, scatter geo doesn't support annotations, so we just create a new plot & add data
# #  https://community.plotly.com/t/how-can-i-combine-choropleth-and-scatter-layer-with-animation-frame-in-a-plotly-map/41330
# d_rename_c_top_city_annot = {
#     'latitude': 'x',
#     'longitude': 'y',
#     'geo_city': 'text',
# }

# d_top_city_annotations = (
#     df_city_and_loc[(
#         (df_city_and_loc['geo_country_code'].isin(l_top_country_codes)) &
#         (df_city_and_loc['city_rank_country'] == 1)
#     )]
#     [[k_ for k_ in d_rename_c_top_city_annot.keys()]]
#     .assign(showarrow=False, xshift=0, yshift=14)
#     .rename(columns=d_rename_c_top_city_annot)
#     .to_dict(orient='records')
# )

# Create base plot with only names of top cities. No markers & no hover
#  TODO(djb): Plot top countries even if they're not in the data(?)
fig_top_cities = px.scatter_geo(
    df_city_and_loc[(
        # top from target countries
        (
            (df_city_and_loc['geo_country_code'].isin(l_top_country_codes)) &
            (df_city_and_loc['city_rank_country'] == 1)
        ) |
        # top from the US; 5 is decent
        (
            (df_city_and_loc['geo_country_code'] == 'US') &
            (df_city_and_loc['city_rank_country'] <= 5)
        )
    )],
    lat="latitude",
    lon='longitude',
    opacity=0,  # set opacity to 0 so we don't see an extra marker
    hover_name=None,
    hover_data=None,
    text='geo_city',
)
# Set hovermode=False so we only see hover info from map with actual city<>subreddit info
fig_top_cities.update_layout(hovermode=False)


# Create custom data for hover with city & subredit info
l_custom_text_ = [
    'city',
    # 'city_rank_country',
    'top_subreddits',
    
    # 'city_rank_world',  # world rank doesn't help much for now
    
]
sub_hovertemplate = "<br>".join(
    [
        "<b>%{customdata[0]}</b>",
        # "<b>Country Rank</b>: %{customdata[1]}"
        # "<b>Popular Subreddits</b>:<br> r/%{customdata[1]}",
        " r/%{customdata[1]}",
    ]
)
fig = px.scatter_geo(
    df_city_map.dropna(how='any'),
    lat="latitude",
    lon='longitude',
    color="country_name",
    hover_name="city", 
    size="city_users_l7_scaled",
    category_orders={'country_name': sorted(df_city_map['country_name'].dropna().unique())},
    custom_data=l_custom_text_,
    # top projection picks: natural earth
    projection="natural earth"
)


fig.update_layout(
    # annotations=d_top_city_annotations,  # annotations don't work with geoscatter()
    width=980,
    height=620,
    autosize=True,
    # yaxis=dict(showgrid=False, zeroline=False,),
    # xaxis=dict(showgrid=False, zeroline=False),
    # plot_bgcolor='#040404',  # dark-gray: '#1a1a1a' '#fcfcfc'
)
# Update hover info
fig.update_traces(
    hovertemplate=sub_hovertemplate,
)
fig.add_trace(fig_top_cities.data[0])

fig.show()



