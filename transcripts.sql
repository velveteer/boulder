SELECT
CAST(chatMessageID as varchar(255)) as messageID,
CAST(chatID as varchar(255)) as chatID,
name,
text,
created AS createdAt
FROM ChatMessageInfo
WHERE chatID = @chatID
