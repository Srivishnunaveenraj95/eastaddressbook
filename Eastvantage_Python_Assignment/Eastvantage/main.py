import math,logging

from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String,Double,TIMESTAMP,Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

log_path = './app.custom.out.stderr.log'
logger = logging.getLogger(__name__)
logging.basicConfig(filename=log_path,
                      format='%(asctime)s -%(levelname)s - %(message)s',
                     level=logging.INFO)
# Create the FASTAPI app
app = FastAPI()
from sqlalchemy import MetaData

metadata_obj = MetaData()
# Configure the database connection
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
print(1)
from datetime import datetime
# from typing import List
from pydantic import BaseModel

default_bookaddress_lat=41.40338
default_bookaddress_lon=2.17403
class AddressBaseSchema(BaseModel):

    latitude: float=None
    longitude: float=None

    place_name: str=None
    city: str  = None
    country: str  = None
    state: str  = None
    pincode: str  = None
    status: bool = True
    created_date: datetime  = None
    updated_date: datetime  = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
# Define a model
class BookAddress(Base):
    __tablename__ = "bookaddress"
    id = Column(Integer, primary_key=True, index=True,autoincrement=True)
    latitude = Column(Double,nullable=False)
    longitude = Column(Double,nullable=False)
    distance=Column(Double,nullable=False)
    place_name=Column(String(200),nullable=False)
    city=Column(String(100),nullable=False)
    country=Column(String(100),nullable=False)
    state=Column(String(100),nullable=False)
    pincode=Column(String(10),nullable=False)
    status = Column(Boolean, nullable=False, default=True)
    created_date = Column(TIMESTAMP(timezone=True),nullable=False, server_default=func.now())
    updated_date = Column(TIMESTAMP(timezone=True),default=None, onupdate=func.now())

# Create the database tables
Base.metadata.create_all(bind=engine)

# Define API routes
#@app.get("/bookaddress/{bookaddress_id}")
@app.get("/bookaddress")
def get_bookaddress():
    db = SessionLocal()
    try:
        # user = db.query(BookAddress).all(BookAddress.id == bookaddress_id).first()
        user = db.query(BookAddress).all()
        address=[]
        for i in user:
            address.append(i.__dict__)
        print(user)
        if  user==None:
            return {"Error":"Records Not Found"}
        db.close()
        #return {"id": user.id, "name": user.place_name}
        return {"data":address}
    except Exception as e:
        err = {"Error": str(e), "Error_Line_No": str(e.__traceback__.tb_lineno), "Message": "Failed To Create"}
        print(e)
        return err
@app.get("/bookaddressdata/{bookaddress_id}")
def get_bookaddress_data(bookaddress_id:int):
    db = SessionLocal()
    try:
        # user = db.query(BookAddress).all(BookAddress.id == bookaddress_id).first()
        user = db.query(BookAddress).filter(BookAddress.id == bookaddress_id).first()
        if  user==None:
            return {"Error":"Records Not Found"}
        db.close()
        return user.__dict__#{"id": user.id, "name": user.place_name}

    except Exception as e:
        err = {"Error": str(e), "Error_Line_No": str(e.__traceback__.tb_lineno), "Message": "Failed To Create"}
        print(e)
        return err

@app.post("/bookaddress")
def create_bookaddress(data: AddressBaseSchema):
    db = SessionLocal()
    try:
        print(data)
        data_validate=data.dict()
        data.created_date=datetime.now()
        data.status=True
        data.updated_date=None
        if(data_validate.get('latitude',None)!=None and data_validate.get('latitude',None)!="" and data_validate.get('latitude',None)!=str(0)):
            if (data_validate.get('latitude')>=-90 and data_validate.get('latitude')<=90):
                pass
            else:
                raise Exception("Invalid Lattitude")
        else:
            raise Exception("Please Enter The Valid Latitude Number")
        if (data_validate.get('longitude', None) != None and data_validate.get('longitude', None) != "" and data_validate.get('longitude', None) != str(0)):
            if (data_validate.get('longitude') >= -180 and data_validate.get('longitude') <= 180):
                pass
            else:
                raise Exception("Invalid longitude")
        else:
            raise Exception("Please Enter The Valid longitude Number")
        if(data_validate.get('place_name', None) == None or data_validate.get('place_name', None) == "" or data_validate.get('place_name', None) == str(0) or str(data_validate.get('place_name', None))).strip()=="":
            raise Exception("Invalid Place_Name")
        if (data_validate.get('city', None) == None or data_validate.get('city', None) == "" or data_validate.get('city', None) == str(0) or str(data_validate.get('city', None))).strip() == "":
            raise Exception("Invalid City Name")
        if (data_validate.get('country', None) == None or data_validate.get('country', None) == "" or data_validate.get('country', None) == str(0) or str(data_validate.get('country', None))).strip() == "":
            raise Exception("Invalid Country Name")
        if (data_validate.get('state', None) == None or data_validate.get('state', None) == "" or data_validate.get('state', None) == str(0) or str(data_validate.get('state', None))).strip() == "":
            raise Exception("Invalid State Name")
        if (data_validate.get('pincode', None) == None or data_validate.get('pincode', None) == "" or data_validate.get('pincode', None) == str(0) or str(data_validate.get('pincode', None))).strip() == "":
            raise Exception("Invalid Pincode No")

        print(data_validate)
        user_lat=data_validate.get('latitude',0)
        user_lon=data_validate.get('longitude',0)
        distance_cal=6371*(math.acos(math.cos(math.radians(default_bookaddress_lat)))*math.cos(math.radians(user_lat))* \
                           math.cos(math.radians(default_bookaddress_lon)-math.radians(user_lon))+ math.sin(math.radians(default_bookaddress_lat)) + math.sin(math.radians(user_lat)))
        data=data.dict()
        data['distance']=distance_cal
        print("SS="+str(data))
        bookaddress = BookAddress(**data)

        db.add(bookaddress)
        db.commit()
        db.refresh(bookaddress)
        db.close()
        return {"id": bookaddress.id, "name": bookaddress.place_name,"Message":"Successfully Created"}
    except Exception as e:
        db.rollback()
        err={"Error":str(e),"Error_Line_No":str(e.__traceback__.tb_lineno),"Message":"Failed To Create"}
        print(e)
        return err
@app.put("/bookaddress/{bookaddress_id}")
def get_bookaddress(bookaddress_id: int,data:dict):
    db = SessionLocal()
    try:
        book_data = db.query(BookAddress).filter(BookAddress.id == bookaddress_id)
        db_note = book_data.first()

        if not db_note:
            return {"Error":"Records Not Found"}
        data_validate = data
        data['created_date'] = datetime.now()
        data['status'] = True
        data['updated_date'] = datetime.now()
        if (data_validate.get('latitude', None) != None and data_validate.get('latitude',
                                                                              None) != "" and data_validate.get(
                'latitude', None) != str(0)):
            if (data_validate.get('latitude') >= -90 and data_validate.get('latitude') <= 90):
                pass
            else:
                raise Exception("Invalid Lattitude")
        else:
            raise Exception("Please Enter The Valid Latitude Number")
        if (data_validate.get('longitude', None) != None and data_validate.get('longitude',
                                                                               None) != "" and data_validate.get(
                'longitude', None) != str(0)):
            if (data_validate.get('longitude') >= -180 and data_validate.get('longitude') <= 180):
                pass
            else:
                raise Exception("Invalid longitude")
        else:
            raise Exception("Please Enter The Valid longitude Number")
        if (data_validate.get('place_name', None) == None or data_validate.get('place_name',
                                                                               None) == "" or data_validate.get(
                'place_name', None) == str(0) or str(data_validate.get('place_name', None))).strip() == "":
            raise Exception("Invalid Place_Name")
        if (data_validate.get('city', None) == None or data_validate.get('city', None) == "" or data_validate.get(
                'city', None) == str(0) or str(data_validate.get('city', None))).strip() == "":
            raise Exception("Invalid City Name")
        if (data_validate.get('country', None) == None or data_validate.get('country', None) == "" or data_validate.get(
                'country', None) == str(0) or str(data_validate.get('country', None))).strip() == "":
            raise Exception("Invalid Country Name")
        if (data_validate.get('state', None) == None or data_validate.get('state', None) == "" or data_validate.get(
                'state', None) == str(0) or str(data_validate.get('state', None))).strip() == "":
            raise Exception("Invalid State Name")
        if (data_validate.get('pincode', None) == None or data_validate.get('pincode', None) == "" or data_validate.get(
                'pincode', None) == str(0) or str(data_validate.get('pincode', None))).strip() == "":
            raise Exception("Invalid Pincode No")

        print(data_validate)
        user_lat = data_validate.get('latitude', 0)
        user_lon = data_validate.get('longitude', 0)
        distance_cal = 6371 * (math.acos(math.cos(math.radians(default_bookaddress_lat))) * math.cos(math.radians(user_lat)) * \
                    math.cos(math.radians(default_bookaddress_lon) - math.radians(user_lon)) + math.sin(
                math.radians(default_bookaddress_lat)) + math.sin(math.radians(user_lat)))
        data = data
        data['distance'] = distance_cal
        print(data)
        book_data = db.query(BookAddress).filter(BookAddress.id == bookaddress_id).update(data,synchronize_session=False)
        print("Success")
        print(book_data)

        db.commit()
        db.refresh(db_note)
        print("SS=" + str(data))
        db.close()
        return {"id": db_note.id, "name": db_note.place_name,"Message":"Updated Successfully"}
    except Exception as e:
        err = {"Error": str(e), "Error_Line_No": str(e.__traceback__.tb_lineno), "Message": "Failed To Update The  Book Address Data"}
        print(e)
        return err
@app.delete("/bookaddress/{bookaddress_id}")
def get_bookaddress(bookaddress_id: int):
    db = SessionLocal()
    try:
        user = db.query(BookAddress).filter(BookAddress.id == bookaddress_id)
        db_note=user.first()
        name=db_note.place_name
        id=db_note.id
        if not db_note:
            return {"Error": "Records Not Found"}
        user.delete(synchronize_session=False)
        db.commit()
        db.close()
        return {"id": id, "name":name ,"Message":"Deleted Successfully"}
    except Exception as e:
        err = {"Error": str(e), "Error_Line_No": str(e.__traceback__.tb_lineno), "Message": "Failed To Update The  Book Address Data"}
        print(e)
        return err

@app.get("/")
def read_root():
    logger.info('Main Screen Started')
    return {"Hello": "World"}