DROP TABLE IF EXISTS chats;
CREATE TABLE chats (
    chat_id VARCHAR(255) PRIMARY KEY,
    answered_at TIMESTAMP WITH TIME ZONE,
    customer_name VARCHAR(255),
    racker_name VARCHAR(255),
    racker_sso VARCHAR(255),
    cloud_account INTEGER,
    core_account INTEGER,
    email_account INTEGER,
    chat_dept VARCHAR(255),
    chat_rating INTEGER,
    chat_status VARCHAR(255)
);

DROP TABLE IF EXISTS transcripts;
CREATE TABLE transcripts (
    message_id VARCHAR(255) PRIMARY KEY,
    chat_id VARCHAR(255),
    person_type INTEGER,
    name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE,
    text TEXT
)
