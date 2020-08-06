# coding: utf-8
from sqlalchemy import CHAR, Column, Float, ForeignKey, String
from sqlalchemy.dialects.mysql import BIGINT, DATETIME, INTEGER, LONGBLOB, LONGTEXT, TINYINT, VARCHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()
metadata = Base.metadata


class Efmigrationshistory(Base):
    __tablename__ = '__efmigrationshistory'

    MigrationId = Column(String(95), primary_key=True)
    ProductVersion = Column(String(32), nullable=False)


class Actionlog(Base):
    __tablename__ = 'actionlogs'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    ModuleName = Column(VARCHAR(50))
    ActionName = Column(VARCHAR(50))
    ITCode = Column(VARCHAR(50))
    ActionUrl = Column(VARCHAR(250))
    ActionTime = Column(DATETIME(fsp=6), nullable=False)
    Duration = Column(Float(asdecimal=True), nullable=False)
    Remark = Column(LONGTEXT)
    IP = Column(VARCHAR(50))
    LogType = Column(INTEGER(11), nullable=False)


class Fileattachment(Base):
    __tablename__ = 'fileattachments'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    FileName = Column(LONGTEXT, nullable=False)
    FileExt = Column(VARCHAR(10), nullable=False)
    Path = Column(LONGTEXT)
    Length = Column(BIGINT(20), nullable=False)
    UploadTime = Column(DATETIME(fsp=6), nullable=False)
    IsTemprory = Column(TINYINT(1), nullable=False)
    SaveFileMode = Column(INTEGER(11))
    GroupName = Column(VARCHAR(50))
    FileData = Column(LONGBLOB)


class Frameworkdomain(Base):
    __tablename__ = 'frameworkdomains'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    DomainName = Column(VARCHAR(50), nullable=False)
    DomainAddress = Column(VARCHAR(50), nullable=False)
    DomainPort = Column(INTEGER(11))
    EntryUrl = Column(LONGTEXT)


class Frameworkgroup(Base):
    __tablename__ = 'frameworkgroups'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    GroupCode = Column(VARCHAR(100), nullable=False)
    GroupName = Column(VARCHAR(50), nullable=False)
    GroupRemark = Column(LONGTEXT)


class Frameworkrole(Base):
    __tablename__ = 'frameworkroles'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    RoleCode = Column(VARCHAR(100), nullable=False)
    RoleName = Column(VARCHAR(50), nullable=False)
    RoleRemark = Column(LONGTEXT)


class News(Base):
    __tablename__ = 'news'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6),default=datetime.datetime.utcnow)
    UpdateBy = Column(VARCHAR(50))
    NewsTime = Column(LONGTEXT, nullable=False)
    Title = Column(LONGTEXT, nullable=False)
    Content = Column(LONGTEXT, nullable=False)
    Src = Column(LONGTEXT)
    KeywordGroup = Column(LONGTEXT)
    KeywordRule = Column(LONGTEXT)


class Newskeyword(Base):
    __tablename__ = 'newskeywords'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    KeywordGroup = Column(LONGTEXT)
    KeywordRule = Column(LONGTEXT)
    KeywordLevel = Column(INTEGER(11))


class Persistedgrant(Base):
    __tablename__ = 'persistedgrants'

    ID = Column(CHAR(36), primary_key=True)
    Type = Column(VARCHAR(50))
    UserId = Column(CHAR(36), nullable=False)
    CreationTime = Column(DATETIME(fsp=6), nullable=False)
    Expiration = Column(DATETIME(fsp=6), nullable=False)
    RefreshToken = Column(VARCHAR(50))


class Tag(Base):
    __tablename__ = 'tags'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    TagName = Column(LONGTEXT, nullable=False)
    TagType = Column(INTEGER(11), nullable=False)


class Frameworkmenu(Base):
    __tablename__ = 'frameworkmenus'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    PageName = Column(VARCHAR(50), nullable=False)
    ActionName = Column(LONGTEXT)
    ModuleName = Column(LONGTEXT)
    FolderOnly = Column(TINYINT(1), nullable=False)
    IsInherit = Column(TINYINT(1), nullable=False)
    ClassName = Column(LONGTEXT)
    MethodName = Column(LONGTEXT)
    DomainId = Column(ForeignKey('frameworkdomains.ID', ondelete='RESTRICT'), index=True)
    ShowOnMenu = Column(TINYINT(1), nullable=False)
    IsPublic = Column(TINYINT(1), nullable=False)
    DisplayOrder = Column(INTEGER(11), nullable=False)
    IsInside = Column(TINYINT(1), nullable=False)
    Url = Column(LONGTEXT)
    ICon = Column(VARCHAR(50))
    ParentId = Column(ForeignKey('frameworkmenus.ID', ondelete='RESTRICT'), index=True)

    frameworkdomain = relationship('Frameworkdomain')
    parent = relationship('Frameworkmenu', remote_side=[ID])


class Frameworkuser(Base):
    __tablename__ = 'frameworkusers'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    ITCode = Column(VARCHAR(50), nullable=False, unique=True)
    Password = Column(VARCHAR(32), nullable=False)
    Email = Column(VARCHAR(50))
    Name = Column(VARCHAR(50), nullable=False)
    Sex = Column(INTEGER(11))
    CellPhone = Column(LONGTEXT)
    HomePhone = Column(VARCHAR(30))
    Address = Column(VARCHAR(200))
    ZipCode = Column(LONGTEXT)
    PhotoId = Column(ForeignKey('fileattachments.ID', ondelete='RESTRICT'), index=True)
    IsValid = Column(TINYINT(1), nullable=False)

    fileattachment = relationship('Fileattachment')


class Newsrelatedstock(Base):
    __tablename__ = 'newsrelatedstocks'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    StockCode = Column(LONGTEXT, nullable=False)
    StockName = Column(LONGTEXT, nullable=False)
    NewsId = Column(ForeignKey('news.ID', ondelete='CASCADE'), nullable=False, index=True)
    Tags = Column(LONGTEXT)

    news = relationship('News')


class Dataprivilege(Base):
    __tablename__ = 'dataprivileges'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    UserId = Column(ForeignKey('frameworkusers.ID', ondelete='CASCADE'), index=True)
    GroupId = Column(ForeignKey('frameworkgroups.ID', ondelete='CASCADE'), index=True)
    TableName = Column(VARCHAR(50), nullable=False)
    RelateId = Column(LONGTEXT)
    DomainId = Column(ForeignKey('frameworkdomains.ID', ondelete='RESTRICT'), index=True)

    frameworkdomain = relationship('Frameworkdomain')
    frameworkgroup = relationship('Frameworkgroup')
    frameworkuser = relationship('Frameworkuser')


class Frameworkusergroup(Base):
    __tablename__ = 'frameworkusergroup'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    UserId = Column(ForeignKey('frameworkusers.ID', ondelete='CASCADE'), nullable=False, index=True)
    GroupId = Column(ForeignKey('frameworkgroups.ID', ondelete='CASCADE'), nullable=False, index=True)

    frameworkgroup = relationship('Frameworkgroup')
    frameworkuser = relationship('Frameworkuser')


class Frameworkuserrole(Base):
    __tablename__ = 'frameworkuserrole'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    UserId = Column(ForeignKey('frameworkusers.ID', ondelete='CASCADE'), nullable=False, index=True)
    RoleId = Column(ForeignKey('frameworkroles.ID', ondelete='CASCADE'), nullable=False, index=True)

    frameworkrole = relationship('Frameworkrole')
    frameworkuser = relationship('Frameworkuser')


class Functionprivilege(Base):
    __tablename__ = 'functionprivileges'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    RoleId = Column(CHAR(36))
    UserId = Column(CHAR(36))
    MenuItemId = Column(ForeignKey('frameworkmenus.ID', ondelete='CASCADE'), nullable=False, index=True)
    Allowed = Column(TINYINT(1), nullable=False)

    frameworkmenu = relationship('Frameworkmenu')


class Searchcondition(Base):
    __tablename__ = 'searchconditions'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    Name = Column(LONGTEXT)
    UserId = Column(ForeignKey('frameworkusers.ID', ondelete='CASCADE'), nullable=False, index=True)
    Condition = Column(LONGTEXT)
    VMName = Column(LONGTEXT)

    frameworkuser = relationship('Frameworkuser')


class StockBasic(Base):
    __tablename__ = 'stock_basic'

    ID = Column(CHAR(36), primary_key=True)
    CreateTime = Column(DATETIME(fsp=6))
    CreateBy = Column(VARCHAR(50))
    UpdateTime = Column(DATETIME(fsp=6))
    UpdateBy = Column(VARCHAR(50))
    ts_code = Column(LONGTEXT, nullable=False)
    symbol = Column(LONGTEXT, nullable=False)
    name = Column(LONGTEXT, nullable=False)
    area = Column(LONGTEXT)
    industry = Column(LONGTEXT)
    list_date = Column(LONGTEXT)
