
-- Kaggle Datsets Analysis
###########################################################################

##total datasets
SELECT COUNT(DISTINCT(d.Id)) FROM
Datasets as d
WHERE
d.RemovalDate is null AND
d.IsPrivate = 'false' AND
d.CreatedByKernelId is not null


##Total kernel
-- Public kernels created on date


WITH LogAction AS (
SELECT 
  CAST(s.DateCreated AS DATE) DateCreated,
  s.Id ScriptId,
  s.AuthorUserId AuthorUserId,
  s.IsPrivate
  FROM Scripts s
    INNER JOIN Users u ON s.AuthorUserId=u.Id
  WHERE s.AuthorUserId NOT IN (SELECT UserId FROM UserRoles)
    AND ((u.CanBeSeen = 1) OR (u.CanAct = 1))
    AND s.DateCreated >= CAST('2020-01-01' AS DATE)
)

SELECT 
  DateCreated,
  COUNT(DISTINCT CASE WHEN IsPrivate=0 THEN ScriptId ELSE NULL END) AS TotalPublicKernels
FROM LogAction
GROUP BY
  DateCreated
ORDER BY
  DateCreated DESC;




-- Total Downloads and Votes
SELECT d.Id, TotalViews, TotalDownloads, TotalVotes, d.CreationDate
FROM Datasets d 
WHERE IsPrivate = 0 AND TotalDownloads > 100 AND TotalDownloads < 5000;

SELECT d.Id, TotalViews, TotalDownloads, TotalVotes, d.CreationDate
FROM Datasets d 
WHERE IsPrivate = 0



-- MONTHLY DATASET DOWNLOAD
SELECT
  CAST((CAST(DATEPART(yyyy, DatabundleDownloads.DownloadDate) AS VARCHAR) + '-' + CAST(DATEPART(m, DatabundleDownloads.DownloadDate) AS VARCHAR) + '-1') AS Date) Month,
  count(*) TotalDownloads
from Datasets
join DatabundleVersions on DatabundleVersions.DatabundleId = Datasets.DatabundleId
join DatabundleDownloads on DatabundleDownloads.DatabundleVersionId=DatabundleVersions.Id
group by CAST((CAST(DATEPART(yyyy, DatabundleDownloads.DownloadDate) AS VARCHAR) + '-' + CAST(DATEPART(m, DatabundleDownloads.DownloadDate) AS VARCHAR) + '-1') AS Date)
order by CAST((CAST(DATEPART(yyyy, DatabundleDownloads.DownloadDate) AS VARCHAR) + '-' + CAST(DATEPART(m, DatabundleDownloads.DownloadDate) AS VARCHAR) + '-1') AS Date)


-- All Datasets with the most public kernels
SELECT 
  dv.DatasetId, 
  dv.Slug,
  COUNT(DISTINCT(s.Id)) NumKernels
FROM 
  Scripts s
INNER JOIN 
  ScriptVersions sv ON s.Id = sv.ScriptId
INNER JOIN 
  ScriptVersionDatasetSources svds ON svds.ScriptVersionId = sv.Id
INNER JOIN 
  DatasetVersions dv ON svds.SourceDatasetVersionId = dv.Id
WHERE
  s.IsPrivate = 0
GROUP BY 
  dv.DatasetId,
  dv.Slug
ORDER BY 
  COUNT(DISTINCT(s.Id)) 
DESC



### PRIVATE AND PUBLIC DATASETS

SELECT
  d.IsPrivate,
  COUNT(DISTINCT d.Id), -- count unique datasets
  COUNT(DISTINCT d.OwnerUserId)
FROM Datasets d
WHERE
  d.OwnerUserId NOT IN (SELECT UserId FROM UserRoles) -- remove all admin users
GROUP BY d.IsPrivate




-- Dataset views and downloads

Kaggle
SELECT d.Id, TotalViews, TotalDownloads, MaintainerOrganizationId, max(name), max(slug)
FROM Datasets d 
INNER JOIN datasetversions v on d.id = v.datasetid
WHERE IsPrivate = 0
GROUP BY
d.Id, TotalViews, TotalDownloads, MaintainerOrganizationId
ORDER BY TotalDownloads DESC



-- All Competitions in a given month year


select c.directlyresponsibleuserid, OnlyAllowKernelSubmissions as KernelsOnly,c.id,isprivate,o.Name as orgname, competitionname,c.title,dateenabled,deadline,
hasleaderboard,rewardquantity,RewardTypeId,CHS.name,enableteammodels,teammodeldeadline,c.NumPrizes,c.totalteams,c.totalcompetitors,c.totalsubmissions
from competitions c
inner join CompetitionHostSegments chs ON C.CompetitionHostSegmentId = CHS.Id
inner join Organizations o ON c.OrganizationId = o.Id
and c.CompetitionHostSegmentId in ('1','2','3','5','6','8','10')
and c.hasleaderboard = 'true'
and c.dateenabled is not null
and c.directlyresponsibleuserid is not null
order by dateenabled desc
/*Note: Query includes Feature, Research, Playground, Getting Started, & InClass
but excludes anything that doesn't have a DirectlyResponsibleUserId assigned.
Also requires that a dateenabled be recorded.*/





-- Monthly Public Kernel Creations, Datasets vs Competitions

Kaggle
WITH KernelCreations AS (
    SELECT DATEADD(mm, DATEDIFF(mm, '2010-01-01', sv.DateCreated), '2010-01-01') RunMonth,
    c.SourceCompetitionId AS CompetitionId,
    com.Title,
    d.Id AS DatasetId,
    dv.Name
    FROM Scripts s
    INNER JOIN ScriptVersions sv ON s.CurrentScriptVersionId=sv.Id
    LEFT JOIN ScriptVersionCompetitionSources c ON c.ScriptVersionId=sv.Id
    LEFT JOIN Competitions com ON com.Id=c.SourceCompetitionId
    LEFT JOIN ScriptVersionDatasetSources dvds ON dvds.ScriptVersionId=sv.Id
    LEFT JOIN DatasetVersions dv ON dv.Id=dvds.SourceDatasetVersionId
    LEFT JOIN Datasets d ON d.Id=dv.DatasetId
    WHERE s.IsPrivate=0
    AND s.AuthorUserId NOT IN (SELECT UserId FROM UserRoles)
)
SELECT
    RunMonth,
    COUNT(*) AS TotalKernels,
    COUNT(CompetitionId) AS CompetitionKernels,
    COUNT(DatasetId) AS DatasetKernels
FROM KernelCreations
GROUP BY RunMonth




--  Competition Popularity

WITH TeamCount AS (
    SELECT C.Id As CompetitionId, COUNT(C.Id) as TeamsParticipating
    FROM Competitions as C
    INNER JOIN Teams as T on C.Id = T.CompetitionId
    WHERE T.CreatedAfterDeadline=0
      AND T.PublicLeaderboardScore is not null
    GROUP BY C.Id
), FileSizes AS (
    SELECT C.Id As CompetitionId, SUM(BF.ContentLength) as FileSize
    FROM Competitions as C
    INNER JOIN CompetitionDataFiles AS CDF on CDF.CompetitionId = C.Id
    INNER JOIN BlobFiles AS BF ON BF.Id = CDF.BlobFileId
    WHERE CDF.IsPublished=1
    GROUP BY C.Id
), SubmissionCounts AS (
    SELECT C.Id As CompetitionId, COUNT(C.Id) as SubmissionCount
    FROM Competitions as C
    INNER JOIN Teams as T on C.Id = T.CompetitionId
    INNER JOIN Submissions as S on S.TeamId = T.Id
    WHERE T.CreatedAfterDeadline=0
      AND T.PublicLeaderboardScore is not null
    GROUP BY C.Id
), UserCount AS (
    SELECT C.Id As CompetitionId, COUNT(C.Id) as UsersParticipating
    FROM Competitions as C
    INNER JOIN Teams as T on C.Id = T.CompetitionId
    INNER JOIN TeamMemberships as TM on TM.TeamId = T.Id
    WHERE T.CreatedAfterDeadline=0
      AND T.PublicLeaderboardScore is not null
    GROUP BY C.Id
) SELECT C.Id AS [Competition Link], TeamsParticipating, FileSize, 
datediff(dd, C.DateEnabled, C.Deadline) AS CompetitionLength,
C.RewardQuantity, SubmissionCount, UsersParticipating
from TeamCount
INNER JOIN Competitions as C on C.Id = TeamCount.CompetitionId
INNER JOIN FileSizes on C.Id = FileSizes.CompetitionId
INNER JOIN SubmissionCounts on C.Id = SubmissionCounts.CompetitionId
INNER JOIN UserCount on C.Id = UserCount.CompetitionId
WHERE C.DateEnabled is not null
  AND C.IsPrivate=0
  AND C.Deadline < getdate()
  AND C.SiteId IN (1,2,5)
  AND C.HasLeaderboard=1
ORDER BY TeamsParticipating DESC





-- Number of Kernele created by date

WITH LogAction AS (
SELECT 
  CAST(s.DateCreated AS DATE) DateCreated,
  s.Id ScriptId,
  s.AuthorUserId AuthorUserId,
  s.IsPrivate
  FROM Scripts s
    INNER JOIN Users u ON s.AuthorUserId=u.Id
  WHERE s.AuthorUserId NOT IN (SELECT UserId FROM UserRoles)
    AND ((u.CanBeSeen = 1) OR (u.CanAct = 1))
   -- AND s.DateCreated >= CAST('2020-02-01' AS DATE)
)

SELECT 
  DateCreated,
  COUNT(DISTINCT CASE WHEN IsPrivate=0 THEN ScriptId ELSE NULL END) AS TotalPublicKernels
FROM LogAction
GROUP BY
  DateCreated
ORDER BY
  DateCreated DESC;






-- Popular kernels
 SELECT Scripts.Id, Users.Username, CurrentURLSlug, TotalViews, TotalVotes
FROM Scripts INNER JOIN Users
  ON Scripts.AuthorUserId = Users.Id
WHERE TotalVotes > 100
ORDER BY TotalViews DESC;



-- kernels langiages
SELECT
sv.KernelLanguageId, kl.Name, COUNT(DISTINCT K.Id) AS count 
FROM KernelVersions kv
LEFT OUTER JOIN KernelSessions sv ON sv.KernelVersionId = kv.Id
JOIN KernelBlobs b ON kv.KernelBlobId = b.Id
JOIN Kernels k ON k.Id = b.KernelId
JOIN KernelLanguages kl ON kl.Id = sv.KernelLanguageId
GROUP BY sv.KernelLanguageId, kl.Name





-- Monthly forks & views of kernels

WITH kernels AS (
SELECT
  s.TotalViews AS Views,
  s.TotalVotes AS Votes,
  s.Id AS ScriptId,
  sv.Id ScriptVersionId,
  s.AuthorUserId,
  sv.Title
FROM ScriptVersions sv
  INNER JOIN Scripts s ON s.Id=sv.ScriptId
  INNER JOIN ScriptCategories sc ON sc.ScriptId=s.Id
WHERE s.DateCreated > '2019-01-01' AND s.DateCreated < '2019-02-01'
)

SELECT
  k.Title,
  AVG(k.Views) AS TotalViews,
  AVG(k.Votes) AS TotalVotes,
  COUNT(DISTINCT s.Id) AS CountKernels,
  COUNT(DISTINCT s.AuthorUserId) AS CountAuthors
FROM Scripts s
  JOIN kernels k ON s.ForkParentScriptVersionId=k.ScriptVersionId
WHERE
  s.AuthorUserId NOT IN (SELECT UserId FROM AdminTeamMemberships)
GROUP BY k.Title


-- Count of Number of Public Kernels

SELECT COUNT(*) AS NumberofPublicKernels
FROM Scripts
WHERE IsPrivate = 'false' AND IsDeleted = 'false'





## Kernel Only competitions
-- all comps
select SourceCompetitionId  as [Competition Link], count(distinct ScriptId) as NumKernels
FROM ScriptVersions v
INNER JOIN Scripts s ON v.ScriptId=s.Id
INNER JOIN ScriptVersionCompetitionSources c ON c.ScriptVersionId=v.Id
where SourceCompetitionId  in (select id from competitions where competitionhostsegmentid <> 10 and dateenabled is not null)
group by SourceCompetitionId 
order by count(distinct ScriptId) desc;





-- Number of kernels associated with competitions

select SourceCompetitionId  as [Competition Link], count(distinct ScriptId) as NumKernels
FROM ScriptVersions v
INNER JOIN Scripts s ON v.ScriptId=s.Id
INNER JOIN ScriptVersionCompetitionSources c ON c.ScriptVersionId=v.Id
where s.IsPrivate = 0 AND
SourceCompetitionId  in (select id from competitions where competitionhostsegmentid <> 10 and dateenabled is not null)
group by SourceCompetitionId 
order by count(distinct ScriptId) desc;




-- Competitions by forum messages or 
SELECT
DISTINCT C.Title AS CompetitionTitle,
COUNT(DISTINCT FT.Id) AS NumForumTopics,
COUNT(DISTINCT FM.Id) AS NumForumMessages
FROM Competitions AS C
INNER JOIN ForumTopics FT ON FT.ForumId = C.ForumId
INNER JOIN ForumMessages FM ON FM.ForumTopicId = FT.Id
WHERE DateEnabled IS NOT NULL
  AND Deadline IS NOT NULL AND isPrivate = 'false'
GROUP BY   C.Title
ORDER BY NumForumMessages DESC, NumForumTopics DESC



-- competition with or wwithout
select CASE WHEN rewardquantity is NULL or rewardquantity = 0 
THEN 'Competition without price' ELSE 'competition with price' END AS Competiton_type,
 count(*) FROM
 competitions
 WHERE isprivate='false'
 GROUP BY CASE WHEN rewardquantity is NULL or rewardquantity = 0 
THEN 'Competition without price' ELSE 'competition with price' END 


-- Forum topics and forum posts by Competition Id

SELECT
C.Title AS CompetitionTitle,
COUNT(DISTINCT FT.Id) AS NumForumTopics,
COUNT(DISTINCT FM.Id) AS NumForumMessages

FROM Competitions AS C
INNER JOIN ForumTopics FT ON FT.ForumId = C.ForumId
INNER JOIN ForumMessages FM ON FM.ForumTopicId = FT.Id

WHERE DateEnabled IS NOT NULL
  AND Deadline IS NOT NULL
  AND CompetitionHostSegmentId <> 10
  AND C.ID = ##CompetitionId##

GROUP BY   C.Title




-- Number of teams in competition

SELECT TOP 3  * FROM  TEAMS;

select distinct T.TeamName,  count(TM.Id) as NumTeammates
from Teams T 
inner join TeamMemberships TM
on TM.TeamId = T.Id
group by T.Id, T.TeamName
order by NumTeammates DESC



## Most voted forum messages
SELECT TOP 100 
  fmt.ForumMessageId AS [Forum Message Link], 
  COUNT(*) AS TotalThanks,
  fm.PostUserId AS [User Link]
FROM ForumMessageVotes fmt, ForumMessages fm
WHERE fmt.ForumMessageId = fm.Id
GROUP BY fmt.ForumMessageId, fm.PostUserId
ORDER BY COUNT(*) DESC


#Most Viewed Forum Messages
SELECT TOP 100 
  ft.TotalViews,
  ft.Name,
  fm.PostUserId AS [User Link],
  u.DisplayName
FROM ForumTopics ft, ForumMessages fm, Users u
WHERE ft.Id = fm.Id
AND fm.Id = u.Id
ORDER BY ft.TotalViews DESC



#most viewed forum messages 

SELECT TOP 100 
  fm.message,
  ft.TotalViews, 
  fm.PostUserId,
  u.DisplayName
 --u.LegalName
FROM ForumTopics ft, ForumMessages fm, Users u
WHERE ft.Id = fm.Id
AND fm.Id = u.Id
ORDER BY ft.TotalViews DESC



-- Most Thanked Forum Messages

SELECT TOP 200
fmt.ForumMessageId AS [Forum Message Link], fm.message,
  COUNT(*) AS TotalThanks,
  fm.PostUserId AS [User Link]
FROM ForumMessageVotes fmt, ForumMessages fm
WHERE fmt.ForumMessageId = fm.Id and VoteDate > '01-01-2010'
GROUP BY fmt.ForumMessageId, fm.PostUserId, fm.message
ORDER BY COUNT(*) DESC




-- User messages to each other
select FromUserId as [User Link], ToUserId as [User Link], Title, Content, usermessages.DateCreated
from usermessages
inner join posts on posts.id = usermessages.postid 
order by usermessages.datecreated desc




-- forum messages counts and average length
SELECT year(postdate) as Year, count(message) as Count_of_messages,
avg(len(message)) as average_length
from forummessages 
group by year(postdate) 




WITH ForumVotes AS (
  SELECT ForumMessageId, SUM(Score) AS NumVotes
  FROM ForumMessageVotes
  GROUP BY ForumMessageId
)
SELECT FM.ForumTopicId, FM.PostUserId, FM.Message, FM.PostDate, FV.NumVotes
FROM Forums F
INNER JOIN ForumTopics FT
ON FT.ForumId = F.Id
INNER JOIN ForumMessages FM
ON FM.ForumTopicId = FT.Id
LEFT OUTER JOIN ForumVotes FV
ON FM.Id = FV.ForumMessageId
WHERE F.Id = (SELECT ForumId FROM Competitions WHERE Id = ##CompId##)
ORDER BY FM.PostDate DESC

-- datasets forum length
select  datasetversions.slug, avg(len(forummessages.message)) from 
datasets
inner join
datasetversions
on Datasets.Id = datasetversions.DatasetId
inner join 
Forums on Datasets.forumId = Forums.Id
inner join
forummessages  on Forums.Id= forummessages.Id
WHERE YEAR(ForumMessages.PostDate) > 2010 AND IsPrivate='TRUE'
GROUP BY datasetversions.slug



# Datasets forum raw 

select ForumMessages.PostDate, datasets.Id, datasets.forumId, datasetversions.slug,forummessages.message  from 
datasets
inner join
datasetversions
on Datasets.Id = datasetversions.DatasetId
inner join 
Forums on Datasets.forumId = Forums.Id
inner join
forummessages  on Forums.Id= forummessages.Id
WHERE (YEAR(ForumMessages.PostDate) between 2016 AND 2018)
AND IsPrivate='FALSE'



# Dataset name, message_length, total forum message 
SELECT v.name, avg(len(forummessages.message)) as average_message_length, 
COUNT(CASE WHEN f.TotalForumMessages IS NULL THEN 0 ELSE f.TotalForumMessages END) TotalForumMessages
FROM Datasets d
INNER JOIN DatasetVersions v ON d.CurrentDatasetVersionId = v.Id
INNER JOIN DatasetVersions dvall ON dvall.DatasetId=d.Id
INNER JOIN Forums f ON d.ForumId = f.Id
inner join
forummessages  on f.Id= forummessages.Id
GROUP BY v.name
ORDER BY TotalForumMessages DESC




#### count of users posting 
select year(postdate) as year,  count(distinct PostUserid) as users
from forummessages
group by year(postdate)


##Average Submission Size By Competition

SELECT 
c.Title, t.CompetitionId AS [Competition Link], SUM(s.ContentLength) AS TotalSubmissionBytes,
COUNT(*) AS TotalSubmissions, (SUM(s.ContentLength) / COUNT(*)) AS AvgSubmissionBytes
FROM Submissions s
JOIN Teams t ON t.Id = s.TeamId
inner join
competitions c
on t.CompetitionId = c.id
GROUP BY c.Title, t.CompetitionId
HAVING SUM(s.ContentLength) > 0
ORDER BY SUM(s.ContentLength) DESC




##### Top Datasets by download
SELECT d.Id, d.Forumid, TotalViews, TotalDownloads, TotalVotes,   max(name) as name, max(slug) as slug
FROM Datasets d 
INNER JOIN datasetversions v on d.id = v.datasetid
WHERE IsPrivate = 0
GROUP BY d.Id,d.Forumid, TotalViews, TotalDownloads, TotalVotes
ORDER BY TotalDownloads DESC, TotalViews DESC,  TotalVotes DESC



-- forum message length

-- Dataset name, message_length, total forum message 
SELECT f.Id, avg(len(forummessages.message)) as average_message_length, 
COUNT(CASE WHEN f.TotalForumMessages IS NULL THEN 0 ELSE f.TotalForumMessages END) TotalForumMessages
FROM Datasets d
INNER JOIN DatasetVersions v ON d.CurrentDatasetVersionId = v.Id
INNER JOIN DatasetVersions dvall ON dvall.DatasetId=d.Id
INNER JOIN Forums f ON d.ForumId = f.Id
inner join
forummessages  on f.Id= forummessages.Id
GROUP BY  f.Id
ORDER BY TotalForumMessages DESC










