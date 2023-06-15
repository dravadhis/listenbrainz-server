/* 
	List of fields in a JSON payload of a listen
	user_id				Int64
	user_name			String
	timestamp			Int64
	track_metadata			JSON
	recording_msid			String
	
	track_metadata is a JSON object with fields:
	
	artist_mbids 			Array(String),
	releases_group_mbid 		String,
	release_mbid 			String,
	recording_mbid 			String,
	track_mbid 			String,
	work_mbids 			Array(String),
	tracknumber 			Int64,
	isrc 				String,
	spotify_id 			String,
	tags 				Array(String),
	media_player 			String,
	media_player_version 		String,
	submission_client 		String,
	submission_client_version 	String,
	music_service 			String,
	music_service_name 		String,
	origin_url 			String,
	duration_ms 			Int64,
	listened_from 			String
*/

/* JSON object type is still an experimental feature */
SET allow_experimental_object_type=1;

DROP DATABASE IF EXISTS lbrmq;

/* Create database for the items connected to RabbitMQ */
CREATE DATABASE lbrmq;

/* 
	This table consumes messages from unique exchange one at a time. SELECT 
	is not useful for reading messages because each message can only be 
	read once. Real time threads can be created using materialized views.
	
	Note that timestamp corresponds to listened_at.
*/
CREATE TABLE IF NOT EXISTS lbrmq.listens_from_unique
(
 `user_id` Int64,
 `user_name` String,
 `timestamp` Int64,
 `track_metadata` String,
 `recording_msid` String
) 
ENGINE = RabbitMQ
SETTINGS 
 rabbitmq_host_port = 'rabbitmq:5672',
 rabbitmq_exchange_name = 'unique',
 rabbitmq_exchange_type = 'fanout',
 rabbitmq_format = 'JSONEachRow',
 rabbitmq_num_consumers = 1;

/* 
	Table to store the data from the rmq messages via the materialized view.
	Note that track_metadata has been JSON Object type. This way has been
	used because RabbitMQ Table Engine does not support the JSON type (it 
	does not support dynamic types in general)
*/
CREATE TABLE IF NOT EXISTS lbrmq.listen
(
 `user_id` Int64,
 `user_name` String,
 `timestamp` Int64,
 `track_metadata` JSON,
 `recording_msid` String
) 
ENGINE = MergeTree()
ORDER BY user_id;

/*
	Materialized view to create real time thread of messages obtained from 
	RabbitMQ
*/
CREATE MATERIALIZED VIEW lbrmq.consumer TO lbrmq.listen AS SELECT * FROM lbrmq.listens_from_unique;
