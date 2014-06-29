SELECT
CAST(c.chatID as varchar(255)) as chat_id,
MAX(c.answered) as answered_at,
MAX(c.chatName) AS customer_name,
MAX(o.name) as racker_name,
MAX(o.userName) AS racker_sso,
MAX(tmp.DDI) AS cloud_account,
MAX(c.visitInfo) AS core_account,
MAX(tmp.EA) AS email_account,
MAX(d.Name) as chat_dept,
MAX(tmp.NPS) AS chat_rating,
MAX(s.name) AS chat_status
FROM ChatInfo c
INNER JOIN OperatorInfo o ON c.operatorID = o.operatorID
INNER JOIN DepartmentInfo d ON c.departmentID = d.departmentID
INNER JOIN SetupItemInfo s ON c.userStatusID = s.setupItemID
INNER JOIN
    (SELECT cf.chatID,
    (SELECT cf.value WHERE cf.name = 'DDI') AS DDI,
    (SELECT cf.value WHERE cf.name = 'AccountNumber') AS EA,
    (SELECT cf.value WHERE cf.name = 'NPS Rating') AS NPS
    FROM ChatCustomFieldsInfo cf
        INNER JOIN ChatInfo c ON cf.chatID = c.chatID
    WHERE cf.name = 'DDI' OR cf.name = 'NPS Rating' OR cf.name = 'AccountNumber') AS tmp
    ON tmp.chatID = c.chatID
WHERE (DATEPART(yy, c.answered) = @year
AND DATEPART(mm, c.answered) = @month
AND DATEPART(dd, c.answered) = @day)
AND d.Name NOT LIKE '%Sales%'
AND c.answered IS NOT NULL
GROUP BY c.chatID
