-- Minimal seed for the example pipeline (config/pipelines/stackoverflow.yaml).
-- Creates the StackOverflow2010 database and empty versions of the tables the
-- example references, so the end-to-end pipeline can run (loading 0 rows)
-- without requiring the real StackOverflow .bak file.
--
-- Replace this file (or load the official StackOverflow2010 dump separately)
-- to exercise the pipeline with actual data.

IF DB_ID('StackOverflow2010') IS NULL
BEGIN
    CREATE DATABASE StackOverflow2010;
END
GO

USE StackOverflow2010;
GO

IF OBJECT_ID('dbo.Users', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Users (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DisplayName NVARCHAR(40) NULL,
        Reputation INT NOT NULL DEFAULT 0,
        CreationDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        LastAccessDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID('dbo.Posts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Posts (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        OwnerUserId INT NULL,
        PostTypeId INT NOT NULL,
        Title NVARCHAR(250) NULL,
        Body NVARCHAR(MAX) NULL,
        CreationDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID('dbo.Comments', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Comments (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PostId INT NOT NULL,
        UserId INT NULL,
        Text NVARCHAR(MAX) NOT NULL,
        CreationDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID('dbo.Badges', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Badges (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        UserId INT NOT NULL,
        Name NVARCHAR(50) NOT NULL,
        Date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID('dbo.Votes', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Votes (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PostId INT NOT NULL,
        VoteTypeId INT NOT NULL,
        CreationDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID('dbo.PostLinks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PostLinks (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PostId INT NOT NULL,
        RelatedPostId INT NOT NULL,
        LinkTypeId INT NOT NULL,
        CreationDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID('dbo.LinkTypes', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.LinkTypes (
        Id INT NOT NULL PRIMARY KEY,
        Type NVARCHAR(50) NOT NULL
    );
END
GO

IF OBJECT_ID('dbo.PostTypes', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.PostTypes (
        Id INT NOT NULL PRIMARY KEY,
        Type NVARCHAR(50) NOT NULL
    );
END
GO

IF OBJECT_ID('dbo.VoteTypes', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.VoteTypes (
        Id INT NOT NULL PRIMARY KEY,
        Name NVARCHAR(50) NOT NULL
    );
END
GO

-- A handful of rows per table so an end-to-end docker-compose run has
-- real data to migrate. Idempotent: skips if the table already has rows.
IF NOT EXISTS (SELECT 1 FROM dbo.Users)
BEGIN
    SET IDENTITY_INSERT dbo.Users ON;
    INSERT INTO dbo.Users (Id, DisplayName, Reputation, CreationDate, LastAccessDate) VALUES
        (1, 'alice',    100, '2025-01-01', '2026-04-01'),
        (2, 'bob',      250, '2025-02-01', '2026-04-15'),
        (3, 'carol',     42, '2025-03-01', '2026-04-20'),
        (4, 'dave',    1024, '2025-04-01', '2026-05-01'),
        (5, 'eve',        7, '2025-05-01', '2026-05-10');
    SET IDENTITY_INSERT dbo.Users OFF;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.PostTypes)
BEGIN
    INSERT INTO dbo.PostTypes (Id, Type) VALUES (1, 'Question'), (2, 'Answer');
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.Posts)
BEGIN
    SET IDENTITY_INSERT dbo.Posts ON;
    INSERT INTO dbo.Posts (Id, OwnerUserId, PostTypeId, Title, Body, CreationDate) VALUES
        (1, 1, 1, 'first question', 'body1', '2026-01-01'),
        (2, 2, 2, 'first answer',   'body2', '2026-01-02'),
        (3, 1, 1, 'second question','body3', '2026-02-01'),
        (4, 3, 2, 'second answer',  'body4', '2026-02-02');
    SET IDENTITY_INSERT dbo.Posts OFF;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.Comments)
BEGIN
    SET IDENTITY_INSERT dbo.Comments ON;
    INSERT INTO dbo.Comments (Id, PostId, UserId, Text, CreationDate) VALUES
        (1, 1, 2, 'nice question', '2026-01-03'),
        (2, 2, 1, 'thanks',        '2026-01-04');
    SET IDENTITY_INSERT dbo.Comments OFF;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.VoteTypes)
BEGIN
    INSERT INTO dbo.VoteTypes (Id, Name) VALUES (2, 'UpMod'), (3, 'DownMod');
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.LinkTypes)
BEGIN
    INSERT INTO dbo.LinkTypes (Id, Type) VALUES (1, 'Linked'), (3, 'Duplicate');
END
GO

PRINT 'StackOverflow2010 seed complete.';
GO
