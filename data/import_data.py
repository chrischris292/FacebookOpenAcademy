import predictionio
import argparse
import simplejson
import json
import pprint
import json
import bson
from bson import Binary, Code
from bson.json_util import loads
import re
import codecs

categories = [];
def __unicode__(self):
   return unicode(self.some_field) or u''

def sendData(args):
    # A user rates an item
    USER_ID = ""
    ITEM_ID = ""
    interestCounter = 0;

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500
    )
    file = codecs.open(args.file, "r",encoding='utf-8')
    line = file.readline();
    counter = 1;
    jsonStr = "";
    while line !="":
      while line !="\n":
        #cite from http://stackoverflow.com/questions/11867538/how-can-i-use-python-to-transform-mongodbs-bsondump-into-json
        #remove all objectID wrappers
        line = re.sub(r'ObjectId\s*\(\s*\"(\S+)\"\s*\)',
                      r'{"$oid": "\1"}',
                      line)

        #remove all ISODate wrappers
        line = re.sub(r'ISODate\s*\(\s*(\S+)\s*\)',
                      r'{"$date": \1}',
                      line)
        
        jsonStr +=line;

        line = file.readline();

      if line== "\n":
        jsonCount = file.readline();
        #Load JSON Obj
        if counter != 1:
          jsonTemp = loads(jsonStr)
          id = jsonTemp["_id"];
          id = jsonTemp["_id"];
          adInterests = [];
          for i in jsonTemp["adgroup"]["targeting"]["interests"]:
            adInterests.append(i["name"])
          descriptionTemp = jsonTemp["description"];
          messageTemp = jsonTemp["message"] 
          if(jsonTemp["description"]==None):
            descriptionTemp = ""
          if(jsonTemp["message"]==None):
            messageTemp = ""
          phrase =  descriptionTemp + " " +  messageTemp;
          #Read through each interest and create event
          for interest in adInterests:
            print interest
            print phrase
            
            response = client.create_event(
              event="$set",
              entity_type="phrase",
              entity_id=jsonTemp["adgroup"]["id"],
              properties= { "phrase" : phrase.encode(encoding='utf8'),
                           "Interest" : interest
              }
            )
            
          #Clear jsonStr when finished loading JSON Object
        jsonStr = ""
        counter +=1;
        line = file.readline();

    print "done"

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for classification engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="data.json")

  args = parser.parse_args()
  print args
  sendData(args)
"""
client = predictionio.EventClient(
    access_key=<ACCESS KEY>,
    url=<URL OF EVENTSERVER>,
    threads=5,
    qsize=500
)
"""