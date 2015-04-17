import predictionio
import argparse

def sendData(args):
    # A user rates an item
    USER_ID = ""
    ITEM_ID = ""
    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500
    )
    file = open(args.file, "r")
    line = file.readline();
    counter = 1;
    
    while line!="": 
      line = line.split(" ");
      interest =  str(" ".join(line[len(line)-1]).replace("\n","").replace(" ",""));
      subString =  str(" ".join(line[0:len(line)-1]));      
      response = client.create_event(
          event="$set",
          entity_type="phrase",
          entity_id=counter,
          properties= { "phrase" : subString,
                        "Interest" : interest
          }
      )
      print response
      print counter;
      
      #Read New Line
      counter = counter + 1;
      line = file.readline();
if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for classification engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="train.txt")

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