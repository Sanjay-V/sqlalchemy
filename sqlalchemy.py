from sqlalchemy import and_ , Column, create_engine, Integer, join, String
from sqlalchemy.orm import foreign, mapper, relationship, Session
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
class Actor_Score(Base):
    __tablename__ = "ActorScore"
    name = Column(String, primary_key=True)
    movie = Column(String)
    year = Column(Integer)
    character = Column(String)
    audience_score = Column(Integer)
    def __repr__(self):
        return "Actor_Score(%s, %s, %s, %s, %s)" % (self.name, self.movie, self.year, self.character, self.audience_score)
class Actor_Performance(Base):
    __tablename__ = "ActorPerformance"
    name = Column(String, primary_key=True)
    movie = Column(String, primary_key=True)
    dance = Column(Integer)
    fight = Column(Integer)
    entertainment = Column(Integer)
    overall = Column(Integer)
    def __repr__(self):
        return "Actor_Performance(%s, %s, %s, %s)" % (self.dance, self.fight, self.entertainment, self.overall)
class Box_Office(Base):
    __tablename__ = "BoxOffice"
    movie = Column(String, primary_key=True)
    box_office_score = Column(Integer, primary_key=True)
    def __repr__(self):
        return "Box_Office(%s)" % (self.box_office_score)
j = join(Box_Office, Actor_Performance, Box_Office.movie == Actor_Performance.movie)
partitioned_second = mapper(
    Box_Office,
    j,
    non_primary=True,
    properties={
        "movie": [j.c.BoxOffice_movie, j.c.ActorPerformance_movie]
    },
)
Actor_Score.BoxOffice = relationship(
    partitioned_second,
    primaryjoin=and_(
        Actor_Score.name == partitioned_second.c.box_office_score,
        Actor_Score.name == foreign(partitioned_second.c.name),
    ),
    innerjoin=True,
)
e = create_engine("sqlite:///ten.sqlite", echo=True)
Base.metadata.create_all(e)
s = Session(e)
s.add_all(
    [
        Actor_Score(name="Vijay", movie="Thupakki", year=2012, character="Actor", audience_score=10),
        Actor_Score(name="Dhanush", movie="Asuran", year=2019, character="Actor", audience_score=9),
        Actor_Score(name="Vivek", movie="Singam", year=2010, character="Comedian", audience_score=8),
        Actor_Score(name="Samantha", movie="Kaththi", year=2014, character="Actress", audience_score=9),
        Actor_Score(name="AR Rahman", movie="Enthiran", year=2010, character="Music Director", audience_score=9),
        Actor_Performance(name="Vijay", movie="Thupakki", dance=9, fight=9, entertainment=8, overall=9),
        Actor_Performance(name="Dhanush", movie="Asuran", dance=8, fight=10, entertainment=8, overall=9),
        Actor_Performance(name="Vivek", movie="Singam", dance=9, fight=9, entertainment=8, overall=9),
        Actor_Performance(name="Samantha", movie="Kaththi", dance=8, fight=2, entertainment=8, overall=8),
        Actor_Performance(name="AR Rahman", movie="Enthiran", dance=8, fight=9, entertainment=8, overall=9),
        Box_Office(movie="Thupakki", box_office_score=9),
        Box_Office(movie="Asuran", box_office_score=9),
        Box_Office(movie="Singam", box_office_score=8),
        Box_Office(movie="Kaththi", box_office_score=8),
        Box_Office(movie="Enthiran", box_office_score=8),
    ]
)
s.commit()
join_query = s.query(Actor_Score, Actor_Performance, Box_Office).join(Actor_Performance, Actor_Performance.name == Actor_Score.name).join(Box_Office, Box_Office.movie == Actor_Score.movie)
for row in join_query.all():
    print("(")
    for item in row:
        print("   ", item)
    print(")")