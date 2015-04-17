# OpenNLP Interests Template

Given a sentence, return a interest category (i.e. Sports, Games, News). 

Interest categories are based off of Facebook Tier 1 interests. 

The engine uses the Apache OpenNLP library

## Versions

### v0.1.0

- initial version

## import sample data

```
$ python data/import_eventserver.py --access_key <your_access_key> --file data/train.txt
```

Use sample training data that has labeled sentences by category. Discretize the categories and label them accordingly in the event server. (

For example,
```
Russell Wilson is a super bowl quarterback	Sports	
```
In the python script associate sports with 1
```
if interest == "Sports":
interest = str(1)
```
Update the interest.scala code to associate sports with 1

```
val Sports = Value(1)
```
## Step to build, train and deploy the engine

```
$ pio build && pio train && pio deploy
```

## Query

The query takes a `String` `sentence`. The result contains a `Interest` under the following categories: `Business_and_Industry`, `Entertainment`, `Business_and_Industry`, `Family_and_Relationships`, `Fitness_and_Wellness`, `Food_and_Drink`, `Hobbies_and_Activities`, `Shopping_and_Fashion`, `Technology`. 

normal:

```
$ curl -H "Content-Type: application/json" \
-d '{
"sentence" : "Best Buy"
}' \
http://localhost:8000/queries.json 

{"interest":"Business"}
```

