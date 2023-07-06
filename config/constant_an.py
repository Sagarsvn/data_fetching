ACTOR_AN_RENAME = dict(actor_id="actor_id:String(single)", actor_name="actor_name:String(single)",
                       created_at="actor_created_at:String(single)",
                       updated_at="actor_updated_at:String(single)")
DIRECTOR_AN_RENAME = dict(director_id="director_id:String(single)", director_name="director_name:String(single)",
                          created_at="director_created_at:String(single)",
                          updated_at="director_updated_at:String(single)")

WRITER_AN_RENAME = dict(writer_id="writer_id:String(single)", writer_name="writer_name:String(single)",
                        created_at="writer_created_at:String(single)",
                        updated_at="writer_updated_at:String(single)")

GENRE_AN_RENAME = dict(genre_id="genre_id:String(single)", genre_name="genre_name:String(single)",
                       created_at="genre_created_at:String(single)",
                       updated_at="genre_updated_at:String(single)")

PROGRAM_DROP_PROPERTY = ['actor_id', 'director_id', 'writer_id']

PROGRAM_AN_RENAME = dict(program_id="program_id:String(single)", program_title='program_title:String(single)',
                         program_summary='program_summary:String(single)',
                         program_release_date='program_release_date:String(single)',
                         program_expired_date='program_expired_date:String(single)',
                         program_status='program_status:String(single)', program_category=
                         'program_category:String(single)',
                         program_created_at='program_created_at:String(single)',
                         program_updated_at='program_updated_at:String(single)'
                         )

EPISODE_AN_RENAME = dict(episode_id='episode_id:String(single)', episode_title='episode_title:String(single)',
                         episode_summary='episode_summary:String(single)',
                         episode_release_date='episode_release_date:String(single)',
                         program_id='program_id:String(single)',
                         episode_expired_date='episode_expired_date:String(single)', episode_status=
                         'episode_status:String(single)',
                         episode_duration='episode_duration:Int(single)',
                         episode_season=' episode_season:String(single)',
                         episode_number='episode_number:String(single)',
                         episode_created_at='episode_created_at:String(single)',
                         episode_updated_at='episode_updated_at:String(single)')

CLIP_AN_RENAME = dict(clip_id='clip_id:String(single)', clip_title='clip_title:String(single)',
                      summary='clip_summary:String(single)',
                      clip_release_date='clip_release_date:String(single)',
                      program_id='program_id:String(single)',
                      clip_expired_date='clip_expired_date:String(single)',
                      clip_status='clip_status:String(single)',
                      clip_duration='clip_duration:Int(single)',
                      clip_created_at="clip_created_at:String(single)",
                      clip_updated_at="clip_updated_at:String(single)")

EXTRA_AN_RENAME = dict(extra_id='extra_id:String(single)',
                       extra_title='extra_title:String(single)',
                       program_id='program_id:String(single)',
                       extra_summary='extra_summary:String(single)',
                       extra_release_date='extra_release_date:String(single)',
                       extra_expired_date='extra_expired_date:String(single)',
                       extra_status='extra_status:String(single)',
                       extra_duration='extra_duration:Int(single)',
                       extra_created_at='extra_created_at:String(single)',
                       extra_updated_at='extra_updated_at:String(single)')

# PROGRAM
PROGRAM_DEPENDENCIES = ['~id', 'actor_id', 'director_id', 'writer_id', 'genre_id', 'program_id']

PROGRAM_DEPENDENCIES_DROP_COLUMN = ['actor_id', 'director_id', 'writer_id', 'genre_id']

ACTOR_DEPENDENCIES_RENAME = {'actor_id:String(single)': 'actor_id', '~id': '~to'}

ACTOR_DEPENDENCIES_REQUIRED = ['actor_id', '~to']

WRITER_DEPENDENCIES_REQUIRED = ['writer_id', '~to']

WRITER_DEPENDENCIES_RENAME = {'writer_id:String(single)': 'writer_id', '~id': '~to'}

GENRE_DEPENDENCIES_RENAME = {'genre_id:String(single)': 'genre_id', '~id': '~to'}

GENRE_DEPENDENCIES_REQUIRED = ['genre_id', '~to']

DIRECTOR_DEPENDENCIES_RENAME = {'director_id:String(single)': 'director_id', '~id': '~to'}

DIRECTOR_DEPENDENCIES_REQUIRED = ['director_id', '~to']

CLIP_DEPENDENCIES_RENAME = {'program_id:String(single)': 'program_id', '~id': '~to'}

CLIP_DEPENDENCIES_REQUIRED = ['program_id', '~to']

EPISODE_DEPENDENCIES_RENAME = {'program_id:String(single)': 'program_id', '~id': '~to'}

EPISODE_DEPENDENCIES_REQUIRED = ['program_id', '~to']

EXTRA_DEPENDENCIES_RENAME = {'program_id:String(single)': 'program_id', '~id': '~to'}

EXTRA_DEPENDENCIES_REQUIRED = ['program_id', '~to']

# USER
USER_AN_RENAME = dict(customer_id='customer_id:String(single)', gender='gender:String(single)',
                      customer_status='customer_status:String(Single)',
                      customer_created_on='customer_created_on:String(single)',
                      customer_updated_on='customer_updated_on:String(single)', age='age:Int(single)',
                      cluster_id='cluster_id:Int(single)')

USER_AN_REQUIRED = ['~id', 'customer_preferences']

USER_PREF_RENAME = {'customer_preferences': 'genre_id', '~id': '~from', }

USER_PREF_REQUIRED = ['~from', 'genre_id']

GENRE_RENAME = {'~id': '~to', 'genre_id:String': 'genre_id'}

GENRE_REQUIRED = ['~to', 'genre_id']

UBD_GROUP_BY = ['customer_id', '~from', 'content_type', 'content_id']

USER_CUSTOMER_REQUIRED = ["customer_id"]

PROGRAM_GRAPH_REQUIRED = ["program_id:String(single)", "~id"]

PROGRAM_GRAPH_RENAME = {"program_id:String(single)": "program_id", "~id": "~to"}

EPISODE_GRAPH_RENAME = {"episode_id:String(single)": "episode_id", "~id": "~to",
                        "episode_duration:Int(single)": "content_duration",
                        "program_id:String(single)": "program_id"}

EPISODE_GRAPH_REQUIRED = ["episode_id:String(single)", "~id", "episode_duration:Int(single)",
                          "program_id:String(single)"]

CLIP_GRAPH_REQUIRED = ["clip_id:String(single)", "~id", "clip_duration:Int(single)", "program_id:String(single)"]

CLIP_GRAPH_RENAME = {"clip_id:String(single)": "clip_id", "~id": "~to", "clip_duration:Int(single)": "content_duration",
                     "program_id:String(single)": "program_id"}

EXTRA_GRAPH_REQUIRED = ["extra_id:String(single)", "~id", "extra_duration:Int(single)", "program_id:String(single)"]

EXTRA_GRAPH_RENAME = {"extra_id:String(single)": "extra_id", "~id": "~to",
                      "extra_duration:Int(single)": "content_duration",
                      "program_id:String(single)": "program_id"}

VIEWED_REQUIRED = ["view_frequency", "watch_duration", "created_on", "~from", "~to", "implicit_rating",
                   "percentage_complete"]

VIEWED_RENAME = dict(view_frequency="view_frequency:Int(single)", watch_duration="total_watch_duration:Int(single)",
                     created_on="created_on:String(single)", implicit_rating="implicit_rating:Float(single)",
                     percentage_complete="precentage_complete:Float(single)")
UBD_PROGRAM_GROUP_BY = ['~from', 'program_id', 'customer_id']

GENRE_ID = 'genre_id'
INNER = "inner"
# Define Label
ACTOR = 'actor'
WRITER = 'writer'
DIRECTOR = 'director'
GENRE = 'genre'
PROGRAM = "program"
EPISODE = "episode"
CLIP = "clip"
EXTRA = "extra"
USER = "user"
HAS_ACTOR = "HAS_ACTOR"
HAS_GENRE = 'HAS_GENRE'
HAS_DIRECTOR = 'HAS_DIRECTOR'
HAS_WRITER = 'HAS_WRITER'
HAS_EPISODE = 'HAS_EPISODE'
HAS_EXTRA = 'HAS_EXTRA'
HAS_CLIP = 'HAS_CLIP'
CUSTOMER_PREFERENCE_1 = 'customer_preference_1'
CUSTOMER_PREFERENCE_2 = 'customer_preference_2'
CUSTOMER_PREFERENCE_3 = 'customer_preference_3'
VIEWED = "VIEWED"
# Extension
CSV = '.csv'
PKL = '.pkl'
# Variable
FROM = "~from"
TO = "~to"
CUSTOMER_ID = "customer_id"
VIEW_FREQUENCY = "view_frequency"
DURATION = "duration"
CREATED_ON = "created_on"
VIEW_HISTORY = "view_history"
CUSTOMER_PREFERENCE = 'customer_preferences'
CONTENT_ID = "content_id"
DEFAULT_CLUSTER_ID = -999
WATCH_DURATION = "watch_duration"
IMPLICIT_RATING = "implicit_rating"
REGEX_IN_COLUMN = "actor*|genre*|director*|writer*|customer_id*|cluster_id*"
CLUSTER_ID = "cluster_id"
MEAN_USER = "mean_user"

CLUSTERING_DATA_FILE_PATH = "user_data/clustering_data.pkl"
# update_user profile

USER_PROFILE_RENAME = dict(customer_id='customer_id:String(single)', gender='gender:String(single)',
                           cluster_id='cluster_id:Int(single)')

USER_PROFILE_PREFERENCE_RENAME = dict(preference_customer_preferen_1='customer_preference_1',
                                      preference_customer_preferen_2='customer_preference_2',
                                      preference_customer_preferen_3='customer_preference_3')

USER_PROFILE_DROP = ['age', 'gender_f', 'gender_m', 'gender_nan',
                     'user_similarity_with_centroid', 'mean_user']

UBD_REQUIRED_COLUMN = ['customer_id', 'created_on', 'watch_duration', 'content_type', 'content_id',
                       'percentage_complete']
PERCENTAGE_COMPLETE = "percentage_complete"

CREATED_AT = 'created_at'
UPDATED_AT = 'updated_at'
