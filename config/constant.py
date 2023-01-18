# customer field
CUSTOMER_REQUIRED = ['id', 'dob', 'gender', 'status',
                     'create_at', 'update_at', 'genre']

CUSTOMER_RENAME = dict(id='customer_id', dob='birth_date', status='customer_status', create_at='customer_created_on',
                       update_at='customer_updated_on', genre='customer_preferences')


# video_content_field

PROGRAM_TYPE_REQUIRED = ['id', 'title', 'summary', 'release_date', 'expired_date', 'status', 'category',
                         'starring', 'directors', 'writers', 'genre']

PROGRAM_TYPE_RENAME = dict(id='program_id', title='content_title', summary='content_summary')

EPISODE_RENAME = dict(id='episode_id')

CLIP_RENAME = dict(id='clip_id')

EXTRA_RENAME = dict(id='extra_id')

# static data

ACTOR_REQUIRED = ['id', 'first_name']

DIRECTOR_REQUIRED = ['id', 'full_name', 'created_at']

WRITER_REQUIRED = ['id', 'full_name', 'created_at']

GENRE_REQUIRED = ['id', 'name']

ACTOR_RENAME = dict(id='actor_id', first_name='actor_name')

DIRECTOR_RENAME = dict(id='director_id', first_name='director_name')

WRITER_RENAME = dict(id='writer_id', first_name='writer_name')

GENRE_RENAME = dict(id='genre_id', name='gnere_name')

# user_behaviour_field"
UBD_RENAME = dict(Viewerid='customer_id', StartTime='created_on', ContentId='content_id')
