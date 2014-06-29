SELECT
CAST(chatMessageID as varchar(255)) as message_id,
CAST(chatID as varchar(255)) as chat_id,
personType as person_type,
name,
created AS created_at,
text
FROM ChatMessageInfo
WHERE chatID = @chatID
