ACTOR_AN_RENAME = dict(actor_id="actor_id:String", actor_name="actor_name:String")
DIRECTOR_AN_RENAME = dict(director_id="director_id:String", director_name="director_name:String")

WRITER_AN_RENAME = dict(writer_id="writer_id:String", writer_name="writer_name:String")

GENRE_AN_RENAME = dict(genre_id="genre_id:String", genre_name="genre_name:String")

PROGRAM_DROP_PROPERTY = ['actor_id', 'director_id', 'writer_id']

PROGRAM_AN_RENAME = dict(program_id="program_id:String", program_title='program_title:String',
                         program_summary='program_summary:String',
                         program_release_date='program_release_date:String',
                         progrma_expired_date='program_expired_date',
                         program_status='program_status:String', program_category='program_category:String')

EPISODE_AN_RENAME = dict(episode_id='episode_id:String', episode_title='episode_title:String',
                         episode_summary='episode_summary:String',
                         episode_release_date='episode_release_date:String',
                         program_id='program_id:String',
                         episode_expire_date='episode_expired_date:String', episode_status='episode_status:String',
                         episode_duration='episode_duration:Int',
                         episode_season=' episode_season:String', episode_number='episode_number:String')

CLIP_AN_RENAME = dict(clip_id='clip_id:String', clip_title='clip_title:String', summary='clip_summary:String',
                      clip_release_date='clip_release_date:String', program_id='program_id:String',
                      clip_expired_date='clip_expired_date:String', clip_status='clip_status:String',
                      clip_duration='clip_duration:Int')

EXTRA_AN_RENAME = dict(extra_id='extra_id:String', extra_title='extra_title:String', program_id='program_id:String',
                       extra_summary='extra_summary:String', extra_release_date='extra_release_date:String',
                       extra_expired_date='extra_expired_date:String', extra_status='extra_status:String',
                       extra_duration='extra_duration:Int')

# Define Label
ACTOR = 'actor'
WRITER = 'writer'
DIRECTOR = 'director'
GENRE = 'genre'
PROGRAM = "program"
EPISODE = "episode"
CLIP = "clip"
EXTRA = "extra"

# Extension
CSV = '.csv'
PKL = '.pkl'
