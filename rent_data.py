from requests import get
from logging import DEBUG
from bs4 import BeautifulSoup
from datetime import datetime
from craigslist import CraigslistHousing
from sqlalchemy import create_engine, Column, Integer, BigInteger, Float, String, DateTime, Boolean, exists
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#from config import USER, PASS, DB

CL_DATE_FORMAT = "%Y-%m-%d %H:%M"
CL_RESULTS = 100

#db_engine = create_engine("postgresql+psycopg2://{}:{}@/{}".format(USER, PASS, DB))

db_engine = create_engine("sqlite:///rent.db")

Session = sessionmaker(bind=db_engine)
session = Session()
Base = declarative_base()

class RentalProperty(Base):
    __tablename__ = 'rental_property'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cl_id = Column(BigInteger, unique=True)
    repost_of_id = Column(BigInteger)
    url = Column(String)
    date_updated = Column(DateTime)
    price = Column(Integer)
    state = Column(String)
    metro = Column(String)
    cl_metro = Column(String)
    zipcode = Column(Integer)
    bedrooms = Column(Integer)
    bathrooms = Column(Float)
    sqft = Column(Integer)
    named_location = Column(String)
    coords = Column(String)
    address = Column(String)
    housing_type = Column(String)
    ac_type = Column(String)
    laundry_type = Column(String)
    parking_type = Column(String)
    furnished = Column(Boolean)
    cats_allowed = Column(Boolean)
    dogs_allowed = Column(Boolean)
    security_deposit = Column(Integer)
    title = Column(String)
    details = Column(String)

class RentalRoom(Base):
    __tablename__ = 'rental_room'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cl_id = Column(BigInteger, unique=True)
    repost_of_id = Column(BigInteger)
    date_updated = Column(DateTime)
    price = Column(Integer)
    state = Column(String)
    metro = Column(String)
    cl_metro = Column(String)
    zipcode = Column(Integer)
    named_location = Column(String)
    coords = Column(String)
    address = Column(String)
    housing_type = Column(String)
    ac_type = Column(String)
    laundry_type = Column(String)
    parking_type = Column(String)
    furnished = Column(Boolean)
    cats_allowed = Column(Boolean)
    dogs_allowed = Column(Boolean)
    security_deposit = Column(Integer)
    title = Column(String)
    details = Column(String)

def get_cl_loc():
    locations = []
    
    response = get('https://www.craigslist.org/about/sites')
    soup = BeautifulSoup(response.text, 'html.parser')
    us = soup.findAll('div', {'class': 'colmask'})[0]

    # get each state and the respectives cities per state
    states          = us.findAll('h4')
    states_cities   = us.findAll('ul')

    for i, state in enumerate(states):
        cities = states_cities[i].findAll('li') 
        for city in cities:
            # get site using splits
            site = city.find('a')['href'].replace('/',',')
            site = site.replace('.',',')
            site = site.split(',')[2]

            data = {}
            data['city'] = city.text
            data['state'] = state.text
            data['site'] = site
            locations.append(data)

    return locations

def add_rooms(loc):
    cl_rooms = CraigslistHousing(site="sandiego", 
                                 filters={'private_room': True, 
                                          'min_price': 25,
                                          'max_price': 3500})
    cl_rooms.set_logger(DEBUG)
    rooms = cl_rooms.get_results(limit=CL_RESULTS, geotagged=True, include_details=True)
    for i, room in enumerate(rooms):
        if i % 100 == 0:
            print('{}th room'.format(i))
        rental_room = RentalRoom()

        db_room = session.query(RentalRoom).filter(RentalRoom.cl_id==room['id']).first()
        if db_room is not None:
            print("found existing place")
            continue
        else:
            rental_room.cl_id = room['id']
            rental_room.repost_of_id = room['repost_of']
            rental_room.url = room['url']
            rental_room.date_updated = datetime.strptime(room['last_updated'], CL_DATE_FORMAT)
            rental_room.price = int(room['price'].replace('$','').replace(',',''))
            rental_room.state = loc['state'] 
            rental_room.metro = loc['city']
            if room.get('area'):
                rental_room.sqft = room['area'].replace('ft2','')
            rental_room.named_location = room['where']
            if room.get('geotag'):
                rental_room.coords = str(room['geotag'][0]) + ',' + str(room['geotag'][1])
            else:
                continue
            rental_room.housing_type = room['house_type']
            rental_room.laundry_type = room['laundry_type']
            rental_room.parking_type = room['parking_type']
            rental_room.furnished = room['furnished']
            rental_room.cats_allowed = room['cats_ok']
            rental_room.dogs_allowed = room['dogs_ok']
            rental_room.title = room['name']
            rental_room.details = room['body']

            session.add(rental_room)
            session.commit()

def add_rentals(loc):
    # TODO after some data is built lets remove outliers using the data
    # TODO remove magic numbers
    for num_rooms in range(1,9):
        print("Searching {} rooms in {}".format(num_rooms, loc['city']))
        cl_rentals = CraigslistHousing(site="sandiego", 
                                       category="apa",
                                       filters={'min_bedrooms': num_rooms,
                                                'max_bedrooms': num_rooms,
                                                'min_price': 100,
                                                'max_price': 20000})
        cl_rentals.set_logger(DEBUG)
        rentals = cl_rentals.get_results(limit=CL_RESULTS, geotagged=True, include_details=True)

        for i, rental in enumerate(rentals):
            if i % 100 == 0:
                print('{}th rental'.format(i))
            rental_property = RentalProperty()

            # check if this already is existing in the DB
            prop = session.query(RentalProperty).filter(RentalProperty.cl_id==rental['id']).first()
            print(rental['id'])

            if prop is not None:
                print(rental['last_updated'], prop.date_updated)
                print(type(datetime.strptime(rental['last_updated'], CL_DATE_FORMAT)))
                continue
            else:
                rental_property.cl_id = rental['id']
                rental_property.repost_of_id = rental['repost_of']
                rental_property.url = rental['url']
                rental_property.date_updated = datetime.strptime(rental['last_updated'], CL_DATE_FORMAT)
                rental_property.price = int(rental['price'].replace('$','').replace(',',''))
                rental_property.state = loc['state'] 
                rental_property.metro = loc['city']
                rental_property.bedrooms = rental['bedrooms']
                rental_property.bathrooms = rental['bathrooms']
                if rental.get('area'):
                    rental_property.sqft = rental['area'].replace('ft2','')
                rental_property.named_location = rental['where']
                if rental.get('geotag'):
                    rental_property.coords = str(rental['geotag'][0]) + ',' + str(rental['geotag'][1])
                else:
                    continue
                print(rental)
                rental_property.housing_type = rental['housing_type']
                rental_property.laundry_type = rental['laundry']
                rental_property.parking_type = rental['parking']
                rental_property.furnished = rental['furnished']
                rental_property.cats_allowed = rental['cats_ok']
                rental_property.dogs_allowed = rental['dogs_ok']
                rental_property.title = rental['name']
                rental_property.details = rental['body']

                session.add(rental_property)
                session.commit()


def add_loc_to_db(loc):
    #add_rentals(loc)
    add_rooms(loc)

def main():
    Base.metadata.create_all(db_engine)

    # get all craigslist locations
    cl_loc_dict = get_cl_loc()
    print(cl_loc_dict)

    for location in cl_loc_dict:
        #add_loc_to_db(debug_loc)
        pass

    debug_loc = {'city': 'san diego', 'state': 'California', 'site': 'sandiego'}
    add_loc_to_db(debug_loc)

main()
