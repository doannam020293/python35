from pymongo import MongoClient
from bson.code import Code
from nam_basic import create_connect_to_mongo


db = create_connect_to_mongo()


mapper = Code("""
    function() {
                  for (var key in this) { emit(key, null); }
               }
""")
reducer = Code("""
    function(key, stuff) { return null; }
""")

distinctThingFields = db.FbGroups.map_reduce(mapper, reducer
    , out = {'inline' : 1}
    , full_response = True)
## do something with distinctThingFields['results']