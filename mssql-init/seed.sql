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

PRINT 'StackOverflow2010 seed complete.';
GO
