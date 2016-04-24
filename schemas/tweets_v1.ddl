CREATE EXTERNAL TABLE tweets_v1 (
  -- contributors null,
  -- in_reply_to_user_id null,
  text string,
  created_at string,
  -- in_reply_to_status_id null,
  favorited boolean,
  entities struct <
    user_mentions:  array <
       struct <
        screen_name: string,
        indices:  array<
         int
        >,
        name: string,
        id: int,
        id_str: string
      >
    >,
    -- hashtags  array <>,
    -- urls  array <>
  >,
  -- geo null,
  source string,
  -- place null,
  retweet_count uniontype<string,int>,
  -- in_reply_to_screen_name null,
  truncated boolean,
  -- coordinates null,
  retweeted boolean,
  -- in_reply_to_status_id_str null,
  `user` struct <
    default_profile: boolean,
    statuses_count: int,
    profile_background_image_url: string,
    screen_name: string,
    friends_count: int,
    profile_link_color: string,
    -- follow_request_sent null,
    created_at: string,
    profile_image_url_https: string,
    profile_background_color: string,
    description: string,
    contributors_enabled: boolean,
    lang: string,
    profile_background_tile: boolean,
    profile_sidebar_fill_color: string,
    url: string,
    show_all_inline_media: boolean,
    listed_count: int,
    is_translator: boolean,
    -- following null,
    profile_sidebar_border_color: string,
    protected: boolean,
    profile_background_image_url_https: string,
    time_zone: string,
    location: string,
    name: string,
    -- notifications null,
    profile_use_background_image: boolean,
    favourites_count: int,
    id: int,
    id_str: string,
    default_profile_image: boolean,
    verified: boolean,
    geo_enabled: boolean,
    utc_offset: int,
    profile_text_color: string,
    followers_count: int,
    profile_image_url: string
  >,
  id bigint,
  -- in_reply_to_user_id_str null,
  id_str string,
  possibly_sensitive boolean,
  retweeted_status struct <
    -- contributors null,
    -- in_reply_to_user_id null,
    text: string,
    created_at: string,
    -- in_reply_to_status_id null,
    favorited: boolean,
    -- struct entities:  is empty
    -- geo null,
    source: string,
    -- place null,
    retweet_count: string,
    -- in_reply_to_screen_name null,
    truncated: boolean,
    -- coordinates null,
    retweeted: boolean,
    -- in_reply_to_status_id_str null,
    `user`: struct <
      default_profile: boolean,
      statuses_count: int,
      profile_background_image_url: string,
      screen_name: string,
      friends_count: int,
      profile_link_color: string,
      -- follow_request_sent null,
      created_at: string,
      profile_image_url_https: string,
      profile_background_color: string,
      description: string,
      contributors_enabled: boolean,
      lang: string,
      profile_background_tile: boolean,
      profile_sidebar_fill_color: string,
      -- url null,
      show_all_inline_media: boolean,
      listed_count: int,
      is_translator: boolean,
      -- following null,
      profile_sidebar_border_color: string,
      protected: boolean,
      profile_background_image_url_https: string,
      time_zone: string,
      location: string,
      name: string,
      -- notifications null,
      profile_use_background_image: boolean,
      favourites_count: int,
      id: int,
      id_str: string,
      default_profile_image: boolean,
      verified: boolean,
      geo_enabled: boolean,
      utc_offset: int,
      profile_text_color: string,
      followers_count: int,
      profile_image_url: string
    >,
    id: bigint,
    -- in_reply_to_user_id_str null,
    id_str: string
  >
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/hwa/tables/tweets_v1'