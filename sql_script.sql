create schema if not exists ods;

create table if not exists ods.T_CHAN_M_ECO_CODES (
	eco varchar(3) primary key,
	ru_name varchar(50),
	en_name varchar(50) not null
);

create table if not exists ods.T_CHAN_PLAYERS (
	user_id integer generated always as identity primary key,
	source_platform varchar(10) not null,
	username varchar(50) not null,
	platform_user_id varchar(100) not null,
	unique(source_platform, platform_user_id)
);

create table if not exists ods.T_CHAN_GAMESDATA_STAGING (
    game_id varchar(100) primary key,
    source_platform varchar(11) not null,
    rated boolean,
    variant varchar(50),
    speed varchar(20),
    created_at TIMESTAMP,
    last_move_at TIMESTAMP,
    status varchar(30),
    winner varchar(5),
    moves text,
    eco_code varchar(3),
    white_player_id integer,
    black_player_id integer,
    white_rating smallint,
    black_rating smallint,
    time_initial integer,
    time_increment integer,
    username varchar(50),
	platform_user_id varchar(100)
);

create table if not exists ods.T_CHAN_GAMESDATA (
    game_id varchar(12),
    source_platform varchar(11) not null,
    rated boolean,
    variant varchar(50),
    speed varchar(20),
    created_at TIMESTAMP,
    last_move_at TIMESTAMP,
    status varchar(30),
    winner varchar(5),
    moves text,
    eco_code varchar(3) references ods.T_CHAN_M_ECO_CODES(eco),
    white_player_id integer,
    black_player_id integer,
    white_rating smallint,
    black_rating smallint,
    time_initial integer,
    time_increment integer,
    user_id integer references ods.T_CHAN_PLAYERS(user_id),
	primary key(source_platform, game_id)
);