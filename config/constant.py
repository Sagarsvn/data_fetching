# customer field
CUSTOMER_REQUIRED = ['id', 'dob', 'gender', 'status',
                     'create_at', 'update_at', 'genre']

CUSTOMER_RENAME = dict(id='customer_id', dob='birth_date', status='customer_status', create_at='customer_created_on',
                       update_at='customer_updated_on', genre='customer_preferences')

# video_content_field

PROGRAM_TYPE_REQUIRED = ['id', 'title', 'summary', 'release_date', 'expired_date', 'status', 'category',
                         'starring', 'directors', 'writers', 'genre','created_at','updated_at']

EPISODE_REQUIRED = ['id', 'program_id', 'title', 'summary', 'release_date', 'status',
                    'duration', 'expire_date', 'season', 'episode','created_at','updated_at']

CLIP_REQUIRED = ['id', 'program_id', 'title', 'summary', 'release_date',
                 'duration', 'expired_date', 'status','created_at','updated_at']

EXTRA_REQUIRED = ['id', 'program_id', 'title', 'summary', 'release_date',
                  'duration', 'expired_date', 'status','created_at','updated_at']

PROGRAM_TYPE_RENAME = dict(id='program_id', title='program_title', summary='program_summary',
                           release_date='program_release_date', expired_date='program_expired_date',
                           status='program_status', category='program_category',
                           starring='actor_id', directors='director_id', genre='genre_id', writers='writer_id',
                           created_at='program_created_at', updated_at='program_updated_at'
                           )

EPISODE_RENAME = dict(id='episode_id', title='episode_title', summary='episode_summary',
                      release_date='episode_release_date',
                      expire_date='episode_expired_date', status='episode_status', duration='episode_duration',
                      season='episode_season', episode='episode_number',
                      created_at='episode_created_at', updated_at='episode_updated_at'
                      )

EXTRA_RENAME = dict(id='extra_id', title='extra_title', summary='extra_summary', release_date='extra_release_date',
                    expired_date='extra_expired_date', status='extra_status', duration='extra_duration',
                    created_at='extra_created_at', updated_at='extra_updated_at'
                    )

CLIP_RENAME = dict(id='clip_id', title='clip_title', summary='clip_summary', release_date='clip_release_date',
                   expired_date='clip_expired_date', status='clip_status', duration='clip_duration',
                   created_at='clip_created_at', updated_at='clip_updated_at')

# static data

ACTOR_REQUIRED = ['id', 'first_name','created_at','updated_at']

DIRECTOR_REQUIRED = ['id', 'full_name','created_at','updated_at']

WRITER_REQUIRED = ['id', 'full_name','created_at','updated_at']

GENRE_REQUIRED = ['id', 'name','created_at','updated_at']

ACTOR_RENAME = dict(id='actor_id', first_name='actor_name')

DIRECTOR_RENAME = dict(id='director_id', full_name='director_name')

WRITER_RENAME = dict(id='writer_id', full_name='writer_name')

GENRE_RENAME = dict(id='genre_id', name='genre_name')

# user_behaviour_field"
UBD_RENAME = dict(Viewerid='customer_id', StartTime='created_on', PlayingTime='watch_duration'
                  , ContentType='content_type', ContentId='content_id')

UBD_DROP = ['customer_id', 'created_on', 'content_id', 'content_type', 'watch_duration']