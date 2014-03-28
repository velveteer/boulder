SELECT
CAST(c.chatID as varchar(255)) as chatID,
MAX(c.answered) as answeredAt,
MAX(c.chatName) AS customerName,
MAX(o.name) as rackerName,
MAX(o.userName) AS rackerSSO,
MAX(tmp.DDI) AS cloudAccount,
MAX(c.visitInfo) AS coreAccount,
MAX(d.Name) as chatDept,
MAX(tmp.NPS) AS chatRating,
MAX(s.name) AS chatStatus
FROM ChatInfo c
INNER JOIN OperatorInfo o ON c.operatorID = o.operatorID
INNER JOIN DepartmentInfo d ON c.departmentID = d.departmentID
INNER JOIN SetupItemInfo s ON c.userStatusID = s.setupItemID
INNER JOIN
    (SELECT cf.chatID,
    (SELECT cf.value WHERE cf.name = 'DDI') AS DDI,
    (SELECT cf.value WHERE cf.name = 'NPS Rating') AS NPS
    FROM ChatCustomFieldsInfo cf
        INNER JOIN ChatInfo c ON cf.chatID = c.chatID
    WHERE cf.name = 'DDI' OR cf.name= 'NPS Rating') AS tmp
    ON tmp.chatID = c.chatID
WHERE (DATEPART(yy, c.answered) = @year
AND DATEPART(mm, c.answered) = @month
AND DATEPART(dd, c.answered) = @day)
AND d.Name NOT LIKE '%Sales%'
AND c.answered IS NOT NULL
GROUP BY c.chatID
